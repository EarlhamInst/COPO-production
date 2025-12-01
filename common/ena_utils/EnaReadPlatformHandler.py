import requests
import xml.etree.ElementTree as ET

from requests.exceptions import RequestException

from common.dal.copo_da import EnaReadPlatformCollection
from common.utils.helpers import (
    get_datetime,
    get_not_deleted_flag,
)
from common.utils.logger import Logger


l = Logger()

''''
Example structure of the XML to be parsed

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:com="SRA.common" targetNamespace="SRA.common">
    ...
    
    <xs:complexType name="PlatformType">
        <xs:annotation>
            <xs:documentation> The PLATFORM record selects which sequencing platform and platform-specific runtime parameters. This will be determined by the Center. </xs:documentation>
        </xs:annotation>
        <xs:choice>
            <xs:element name="LS454">
                <xs:annotation>
                    <xs:documentation> 454 technology use 1-color sequential flows </xs:documentation>
                </xs:annotation>
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="INSTRUMENT_MODEL" maxOccurs="1" minOccurs="1" type="com:type454Model"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:choice>
    </xs:complexType>

    <xs:simpleType name="type454Model">
        <xs:restriction base="xs:string">
            <xs:enumeration value="454 GS"/>
            <xs:enumeration value="454 GS 20"/>
            <xs:enumeration value="454 GS FLX"/>
            <xs:enumeration value="454 GS FLX+"/>
            <xs:enumeration value="454 GS FLX Titanium"/>
            <xs:enumeration value="454 GS Junior"/>
            <xs:enumeration value="unspecified"/>
        </xs:restriction>
    </xs:simpleType>
    
    ...
    
</xs:schema>
'''

class EnaReadPlatformHandler:
    def __init__(self):
        self.headers = {
            'Accept': 'application/xml',
        }
        self.namespace = {'xs': 'http://www.w3.org/2001/XMLSchema', 'com': 'SRA.common'}
        self.urls = [
            'https://ftp.ebi.ac.uk/pub/databases/ena/doc/xsd/sra_1_5/SRA.common.xsd'
        ]

    def _load_platform(self, url):
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            return response.text
        except RequestException as e:
            l.exception(f'Request failed: {e}')
            return None
        except ET.ParseError as e:
            l.exception(f'XML parsing failed: {e}')
            return None

    def _parse_platform(self, xml_str):
        dt = get_datetime()

        try:
            root = ET.fromstring(xml_str)

            # Map: simpleType name to the list of dropdown options
            model_map = {}

            # Extract instrument models
            for simple_type in root.findall('.//xs:simpleType', self.namespace):
                name = simple_type.attrib.get('name')

                if name and name.startswith('type'):
                    values = []
                    for enum in simple_type.findall(
                        './/xs:enumeration', self.namespace
                    ):
                        values.append(enum.attrib['value'])

                    model_map[name] = values

            sequencing_instruments = []

            # Find platform definitions and link them
            for platform in root.findall('.//xs:element', self.namespace):
                platform_name = platform.attrib.get('name')
                if not platform_name:
                    continue

                # Look for INSTRUMENT_MODEL inside platform
                model_element = platform.find(
                    ".//xs:element[@name='INSTRUMENT_MODEL']", self.namespace
                )
                if model_element is None:
                    continue

                type_ref = model_element.attrib.get('type')
                if not type_ref:
                    continue

                # Extract local type name e.g. com:type454Model -> type454Model
                type_name = type_ref.split(':')[-1]
                instrument_values = model_map.get(type_name, [])
                
                # Omit 'unspecified' from the list of instruments if present
                instrument_values = [v for v in instrument_values if v.lower() != 'unspecified']

                platform = {
                    'platform': platform_name,
                    'instruments': instrument_values,
                    'modified_date': dt,
                    'deleted': get_not_deleted_flag(),
                }

                sequencing_instruments.append(platform)
            return sequencing_instruments
        except Exception as e:
            l.exception(e)
            return []

    def update_platform(self):
        platform_set = []
        for url in self.urls:
            xml_str = self._load_platform(url)
            platform_set.extend(self._parse_platform(xml_str))

        for platform in platform_set:
            EnaReadPlatformCollection().get_collection_handle().find_one_and_update(
                {'platform': platform['platform']},
                {'$set': platform},
                upsert=True,
            )
