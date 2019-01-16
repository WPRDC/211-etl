import os, sys, csv, json, re, datetime
from marshmallow import fields, pre_load, post_load

from datetime import datetime
from dateutil import parser
from pprint import pprint
sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import pprint
import time
from collections import OrderedDict

from parameters.local_parameters import SETTINGS_FILE, DATA_PATH
from util.notify import send_to_slack
from util.ftp import fetch_files

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        #for d in list_of_dicts:
        #    dict_writer.writerow({k:v.encode('utf8') if type(v) == type('A') else v for k,v in d.items()})
        dict_writer.writerows(list_of_dicts)

def rename_field(dict_i, old_field_name, new_field_name):
    if old_field_name in dict_i:
        dict_i[new_field_name] = str(dict_i[old_field_name])
        del(dict_i[old_field_name])
        return new_field_name
    else:
        #print("Unable to find {} in passed dictionary.".format(old_field_name))
        return None

def int_or_none(item):
    try: 
        val = int(item)
    except:
        return None

def boolify(s):
    if s == 'True':
        return True
    if s == 'False':
        return False
    raise ValueError("huh?")

def autoconvert(s):
    for fn in (boolify, int, float):
        try:
            return fn(s)
        except ValueError:
            pass
    return s

def handle_date_and_time(d,date_field_name,time_field_name):
    hour, minute = d[time_field_name].split(':')
    dt = datetime.strptime(d[date_field_name],'%m/%d/%Y').replace(hour = int(hour), minute = int(minute))
    del(d[time_field_name])
    del(d[date_field_name])
    return dt

def form_key(d,rename):
    if rename:
        return "{} | {}".format(d['contact_record_id'], d['needs_category'])
    else:
        return "{} | {}".format(d['Contact Record ID'], d['Presenting Needs: Taxonomy Category'])

def get_headers(filename):
    with open(filename, 'r') as f:
        headers_string = f.readline()
    fieldnames = headers_string.split(',')
    fieldnames[-1] = fieldnames[-1].rstrip('\n').rstrip('\r')
    return(fieldnames)


class Base211Schema(pl.BaseSchema): 
    created = fields.Date(allow_none=True)
    agency_name = fields.String(allow_none=False)
    contact_record_id = fields.String(allow_none=False)
    client_id = fields.String(allow_none=False)
    age = fields.String(allow_none=True)
    children_in_home = fields.Boolean(allow_none=True)
    contact_medium = fields.String(allow_none=True)
    county = fields.String(allow_none=False)
    region = fields.String(allow_none=False)
    state = fields.String(allow_none=False)
    zip_code = fields.String(allow_none=False)
    gender = fields.String(allow_none=True)
    military_household = fields.String(allow_none=True)
    health_insurance_for_household = fields.String(allow_none=True)
    main_reason_for_call = fields.String(allow_none=True)

    class Meta:
        ordered = True

class NeedsSchema(Base211Schema):
    needs_category = fields.String(allow_none=True)
    code_level_1 = fields.String(allow_none=True)
    code_level_1_name = fields.String(allow_none=True)
    needs_code = fields.String(allow_none=True)
    code_level_2 = fields.String(allow_none=True)
    code_level_2_name = fields.String(allow_none=True)
    were_needs_unmet = fields.String(allow_none=True)
    why_needs_unmet = fields.String(allow_none=True)

class ClientSchema(Base211Schema):
    class Meta:
        ordered = True

class ContactSchema(Base211Schema):

    #amount = fields.Float(dump_to="amount", allow_none=True)
    # Never let any of the key fields have None values. It's just asking for 
    # multiplicity problems on upsert.

    # [Note that since this script is taking data from CSV files, there should be no 
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as as a zero-length string, which this script
    # is then responsible for converting into something more appropriate.


    # From the Marshmallow documentation:
    #   Warning: The invocation order of decorated methods of the same 
    #   type is not guaranteed. If you need to guarantee order of different 
    #   processing steps, you should put them in the same processing method.
    @pre_load
    def plaintiffs_only_and_avoid_null_keys(self, data):
        #if data['party_type'] != 'Plaintiff':
        #    data['party_type'] = '' # If you make these values
        #    # None instead of empty strings, CKAN somehow
        #    # interprets each None as a different key value,
        #    # so multiple rows will be inserted under the same
        #    # DTD/tax year/lien description even though the
        #    # property owner has been redacted.
        #    data['party_name'] = ''
        #    #data['party_first'] = '' # These need to be referred
        #    # to by their schema names, not the name that they
        #    # are ultimately dumped to.
        #    #data['party_middle'] = ''
        #    data['plaintiff'] = '' # A key field can not have value
        #    # None or upserts will work as blind inserts.
        #else:
        #    data['plaintiff'] = str(data['party_name'])
        #del data['party_type']
        #del data['party_name']
    # The stuff below was originally written as a separate function 
    # called avoid_null_keys, but based on the above warning, it seems 
    # better to merge it with omit_owners.
        if data['plaintiff'] is None: 
            data['plaintiff'] = ''
            print("Missing plaintiff")
        if data['block_lot'] is None:
            data['block_lot'] = ''
            print("Missing block-lot identifier")
            pprint.pprint(data)
        if data['pin'] is None:
            data['pin'] = ''
            print("Missing PIN")
            pprint.pprint(data)
        if data['case_id'] is None:
            pprint.pprint(data)
            raise ValueError("Found a null value for 'case_id'")
        if data['docket_type'] is None:
            data['docket_type'] = ''
            pprint.pprint(data)
            print("Found a null value for 'docket_type'")



    @pre_load
    def fix_date_and_bin_age(self, data):
        if data['filing_date']:
            data['filing_date'] = parser.parse(data['filing_date']).date().isoformat()
        else:
            print("No filing date for {} and data['filing_date'] = {}".format(data['dtd'],data['filing_date']))
            data['filing_date'] = None


def bin_age(data):
    """Convert age string to a U.S. Census range of ages. Handle ridiculously large/negative ages and non-integer ages."""
    age = data['age']
    try:
        age = int(age)
        if age < 0:
            data['age'] = None
        elif age < 6:
            data['age'] = '0 to 5'
        elif age < 18:
            data['age'] = '6 to 17'
        elif age < 25:
            data['age'] = '18 to 24'
        elif age < 45:
            data['age'] = '25 to 44'
        elif age < 65:
            data['age'] = '45 to 64'
        elif age < 130:
            data['age'] = '65 and over'
        else: # Observed examples: 220, 889, 15025, 15401, 101214
            data['age'] = None
    except ValueError:
        data['age'] = None

def standardize_date(data):
    if data['created']:
        data['created'] = parser.parse(data['created']).date().isoformat()
    else:
        print("Unable to turn data['created'] = {} into a valid date.".format(data['created']))
        data['created'] = None

def remove_bogus_zip_codes(data):
    """The United Way is coding unknown ZIP codes as 12345. These codes should be converted to blanks
    before we get the data. This function is just a precautionary backstop."""
    if data['zip_code'] == '12345':
        data['zip_code'] = None

def translate_headers(headers, alias_lookup):
    return [alias_lookup[header] for header in headers]

def process(raw_file_location,processed_file_location,filecode,schema):
    """Rename fields, bin ages, and hash IDs here."""
    headers = get_headers(raw_file_location)
    print("headers of {} = {}".format(raw_file_location,headers))

    # Option 1: Parse the whole CSV file, modify the field names, reconstruct it, and output it as a new file.
    just_change_headers = False
    alias_lookup = {'Contact: System Create Date': 'created',
            'Contact: Agency Name': 'agency_name',
            'Contact Record ID': 'contact_record_id',
            'Client ID': 'client_id',
            'Age': 'age',
            'Are there children in the home?': 'children_in_home',
            'Type of Contact': 'contact_medium',
            'County': 'county',
            'Region': 'region',
            'State': 'state',
            'Zip': 'zip_code',
            'Gender': 'gender',
            'Have you or anyone in the household served in the military?': 'military_household',
            'SW - HealthCare - Does everyone in your household have health insurance?': 'health_insurance_for_household',
            'Does everyone in your household have health insurance?': 'health_insurance_for_household',
            'Primary reason for calling': 'main_reason_for_call',
            'Presenting Needs: Taxonomy Category': 'needs_category',
            'Taxonomy L1': 'code_level_1',
            'Taxonomy L1 Name': 'code_level_1_name',
            'Presenting Needs: Taxonomy Code': 'needs_code',
            'Taxonomy L2': 'code_level_2',
            'Taxonomy L2 Name': 'code_level_2_name',
            'Presenting Needs: Unmet?': 'were_needs_unmet',
            'Presenting Needs: Reason Unmet': 'why_needs_unmet'}

    fields_with_types = schema().serialize_to_ckan_fields()
    #fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))

    fields = [f['id'] for f in fields_with_types]
    new_headers = fields

    if not just_change_headers:
        with open(raw_file_location, 'r') as f:
            dr = csv.DictReader(f)
            rows = []
            ds = []
            for d in dr:
                # row is a dict with keys equal to the CSV-file column names
                # and values equal to the corresponding values of those parameters.
                # FIX FIELD TYPES HERE.
                for old_field, new_field in alias_lookup.items():
                    rename_field(d, old_field, new_field)
                del(d['Call Type Detail'])
                bin_age(d)
                standardize_date(d)
                remove_bogus_zip_codes(d)
                ds.append(d)

            write_to_csv(processed_file_location,ds,new_headers)

    else: # option 2: just read the first line of the file, translate the headers, write the headers to a new file and pipe through the rest of the file contents.
        new_headers = translate_headers(headers, alias_lookup)
        with open(processed_file_location, 'w') as outfile:
            with open(raw_file_location, 'r') as infile:
                for k,line in enumerate(infile):
                    if k == 0:
                        outfile.write(','.join(new_headers)+'\n')
                    else:
                        outfile.write(line)

def main(**kwargs):
    schema_by_code = OrderedDict( [('clients', ClientSchema), ('contacts', ContactSchema), ('needs', NeedsSchema)] )

    specify_resource_by_name = True
    if specify_resource_by_name:
        kwparams = {'resource_name': '211 Clients (beta)'}
    #else:
        #kwargs = {'resource_id': ''}
    server = kwargs.get('server','211-testbed')
    output_to_csv = kwargs.get('output_to_csv',False)
    push_to_CKAN = kwargs.get('push_to_CKAN',False)
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
    with open(SETTINGS_FILE) as f: 
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']

    # Get CSV files that contain the data. These will come from 
    # either a remote server or a local cache.
    fetch_data = False
    if fetch_data:
        print("Pulling the latest 2-1-1 data from the source server.")

    # Change path to script's path for cron job
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    local_path = dname + "/raw_data" # This is called "latest_pull" for the foreclosures ETL.
    # It's just an archive of previously obtained raw-data files. fetch_files relies on
    # the filename to change and archives whatever it pulls.

    # If this path doesn't exist, create it.
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    most_recent_path = dname + "/most_recent" # This is where the most recently pulled
    # file will be stored in raw format and then in processed format.

    # If this path doesn't exist, create it.
    if not os.path.exists(most_recent_path):
        os.makedirs(most_recent_path)

    filecodes = ['clients', 'contacts', 'needs']
    processed_file_locations = ["{}/{}.csv".format(most_recent_path,fc) for fc in filecodes]

    if fetch_data:
        search_terms = ['opendata']
        raw_file_locations = fetch_files(SETTINGS_FILE,local_path,DATA_PATH,search_terms)
    else:
        raw_file_locations = ["{}/raw-{}.csv".format(most_recent_path,fc) for fc in filecodes]

    for raw_file_location, processed_file_location,filecode in zip(raw_file_locations,processed_file_locations,filecodes):
        schema = schema_by_code[filecode]
        process(raw_file_location,processed_file_location,filecode,schema)

        if push_to_CKAN:
            print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(processed_file_location,list(kwparams.values())[0],package_id,site))
            time.sleep(1.0)

            fields0 = schema().serialize_to_ckan_fields()
            # Eliminate fields that we don't want to upload.
            #fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
            #fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
            #fields0.append({'id': 'assignee', 'type': 'text'})
            fields_to_publish = fields0
            print("fields_to_publish = {}".format(fields_to_publish))

            ###############
            # FOR SOME PART OF THE BELOW PIPELINE, I THINK...
            #The package ID is obtained not from this file but from
            #the referenced settings.json file when the corresponding
            #flag below is True.
            two_one_one_pipeline = pl.Pipeline('two_one_one_pipeline',
                                              'Pipeline for 2-1-1 Data',
                                              log_status=False,
                                              settings_file=SETTINGS_FILE,
                                              settings_from_file=True,
                                              start_from_chunk=0
                                              )
            two_one_one_pipeline = two_one_one_pipeline.connect(pl.FileConnector, target, encoding='utf-8') \
                .extract(pl.CSVExtractor, firstline_headers=True) \
                .schema(schema) \
                .load(pl.CKANDatastoreLoader, server,
                      fields=fields_to_publish,
                      #package_id=package_id,
                      #resource_id=resource_id,
                      #resource_name=resource_name,
                      key_fields=['case_id','pin','block_lot','plaintiff','docket_type'],
                      # A potential problem with making the pin field a key is that one property
                      # could have two different PINs (due to the alternate PIN) though I
                      # have gone to some lengths to avoid this.
                      method='upsert',
                      **kwparams).run()
            log = open('uploaded.log', 'w+')
            if specify_resource_by_name:
                print("Piped data to {}".format(kwparams['resource_name']))
                log.write("Finished upserting {}\n".format(kwparams['resource_name']))
            else:
                print("Piped data to {}".format(kwparams['resource_id']))
                log.write("Finished upserting {}\n".format(kwparams['resource_id']))
            log.close()

        if output_to_csv:
            print("This is where the table should be written to a CSV file for testing purposes.")

if __name__ == "__main__":
   # stuff only to run when not called via 'import' here
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        output_to_csv = False
        push_to_CKAN = False

        copy_of_args = list(args)

        list_of_servers = ["211-testbed", 
                ] # This list could be automatically harvested from SETTINGS_FILE.

        kwparams = {}
        # This is a new way of parsing command-line arguments that cares less about position
        # and just does its best to identify the user's intent.
        for k,arg in enumerate(copy_of_args):
            if arg in ['scan', 'save', 'csv']:
                output_to_csv = True
                args.remove(arg)
            elif arg in ['pull', 'push', 'ckan']:
                push_to_CKAN = True
                args.remove(arg)
            elif arg in list_of_servers:
                kwparams['server'] = arg
                args.remove(arg)
            else:
                print("I have no idea what do with args[{}] = {}.".format(k,arg))

        kwparams['output_to_csv'] = output_to_csv
        kwparams['push_to_CKAN'] = push_to_CKAN
        print(kwparams)
        main(**kwparams)
    else:
        print("Please specify some command-line parameters next time.")
        main()
