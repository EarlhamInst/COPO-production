from django.conf import settings
from json import loads
from xml.etree.ElementTree import fromstring

import requests
from xmljson import badgerfish as bf


def iterate_over_attributes(od, data_type):
    # extract key values starting with @ symbols
    # print(data_type)
    out = dict()

    for el in od['ROOT'][data_type]:
        if el.startswith('@'):
            # get key and remove '@'
            value = od['ROOT'][data_type][el]
            key = el[1:]
            out[key] = value
    return out


def query_ena(acc):
    # query ENA for primary accession
    # print('querying ena for: ' + acc)
    resp = requests.get(f'http://{settings.ENA_URL_HOSTNAME}/ena/browser/view/' + acc + '%26display%3Dxml')
    ##print('decoding...')
    bt = resp.content
    st = bt.decode('utf-8')
    et = fromstring(st)
    b = bf.data(et)
    return b


def do_sub_element(el_list, el_type):
    # call to ENA for additional elements

    # array of elements to extract
    do_if = ['SUBMISSION', 'STUDY', 'SAMPLE', 'EXPERIMENT']

    out = dict()
    out_list = list()

    if '-' in el_type:
        short_form_type = el_type[el_type.index('-') + 1:]

    if short_form_type in do_if:
        # init output dict
        if type(el_list) == str:
            el_list = [el_list]

        for el in el_list:
            # do lookup
            resp = requests.get(f'http://{settings.ENA_URL_HOSTNAME}/ena/browser/view/' + el + '%26display%3Dxml').content
            st = resp.decode('utf-8')
            et = fromstring(st)
            data = bf.data(et)
            x = iterate_over_attributes(data, short_form_type)
            out_list.append(x)

    return out_list


def query_bio_samples(acc):
    sample = dict()
    # get an additional information accessible through Biosamples
    resp = requests.get(
        f'https://{settings.ENA_URL_HOSTNAME}/biosamples/api/samples/search/findByText?text=' + acc + '%20AND%20external_references_name_crt=ENA')
    samples = loads(resp.content.decode('utf-8'))
    sub = samples['_embedded']['samples'][0]
    sub_chars = sub['characteristics']
    for x in sub:
        if not x.startswith('_'):
            sample[x] = sub[x]
    return sample

def do_import_ena_accession(acc):



    # iteritively convert to correct json format
    data = query_ena(acc)

    # start dict
    out = dict()
    out['GENERAL'] = iterate_over_attributes(data, 'SUBMISSION')
    for sub in data['ROOT']['SUBMISSION']['SUBMISSION_LINKS']['SUBMISSION_LINK']:
        el_type = sub['XREF_LINK']['DB']['$']
        sub_dict = do_sub_element(el_list=sub['XREF_LINK']['ID']['$'], el_type=el_type)

        # do further call to Biosamples if we are dealing with samples
        if el_type == 'ENA-SAMPLE':
            for s in sub_dict:
                acc = s['accession']
                v = query_bio_samples(acc)
                sub_dict[0].update(v)
            out[el_type] = sub_dict

        elif sub_dict:
            out[el_type] = sub_dict

    # print(data)
    return out