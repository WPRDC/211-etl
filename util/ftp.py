import os, json, re, shutil, pysftp
import hashlib

from .notify import send_to_slack

def compute_hash(target_file):
    # Stolen from countermeasures ETL script.
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    with open(target_file, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()


def fetch_files(settings_file,local_landing_path,local_storage_path,search_terms):

    # If the local path doesn't exist, create it.
    if not os.path.exists(local_landing_path):
        print("Creating {} as the local_landing_path for fetch_files.".format(local_landing_path))
        os.makedirs(local_landing_path)

    with open(settings_file) as f:
        settings = json.load(f)
        hostname = settings['connector']['sftp']['county_sftp']['host']
        username = settings['connector']['sftp']['county_sftp']['username']
        password = settings['connector']['sftp']['county_sftp']['password']
        remote_path = settings['connector']['sftp']['county_sftp']['remote_path']
        known_hosts_file = settings['connector']['sftp']['known_hosts']
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.load(known_hosts_file)
    #### Make sure not to overwrite existing files obliviously.
    # There's different scenarios to consider here:
    # 1) There's a file in the main foreclosure_data directory that is outdated and should be
    # overwritten with correct data.
    # 2) There's a file in the main foreclosure_data directory that is correct and the new
    # data should not replace it.
    # (Both these are basically hypothetical scenarios, since so far the files have all been
    # fine and uniquely named, except maybe when we asked for some files that we missed
    # and got some extra months in that update (the updated files were different than the
    # old ones because the data is dependent on when it's pulled).)
    destination_paths = []
    with pysftp.Connection(hostname, username=username, password=password,cnopts=cnopts) as sftp:
        with sftp.cd(remote_path):           # Change directory
            files = sftp.listdir()
            targets = []
            for term in search_terms:
                for fn in files:
                    if re.search(term,fn) is not None:
                        if fn not in targets:
                            targets.append(fn)
            print("targets = {}".format(targets))
            for t in targets:
                # First save the file to a local latest_pull directory.
                save_location = "{}/{}".format(local_landing_path,t)
                sftp.get(t,save_location)
                # Then check whether the filename already exists in the primary storagae directory.
                destination_path = "{}/{}".format(local_storage_path,t)
                if os.path.exists(destination_path):
                    # It's probably fine, unless the files don't match.
                    old_file_hash = compute_hash(destination_path)
                    new_file_hash = compute_hash(save_location)
                    if old_file_hash != new_file_hash:
                        msg = "211-etl: There's a conflict between {} and the new file (residing at {})".format(destination_path,save_location)
                        send_to_slack(msg)
                        raise ValueError(msg)
                    else:
                        print("There is already a file at {}.".format(destination_path))
                        # If the file's already there, is there any point in reuploading it
                        # to the data portal?
                        # Is there any harm in it? [Let's prefer to try to re-upsert it,
                        # just in case something went wrong last time.]
                else: #   If no file is at the destination already, copy the new file over.
                    shutil.copy(save_location,destination_path)
                    print("Copied the file from the FTP server to the archive directory.")
                destination_paths.append(destination_path)

    return destination_paths
