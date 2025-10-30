import csv
import datetime
import pathlib
import sys
import unicodedata
from collections import namedtuple, defaultdict

import openpyxl

from cldfbench import CLDFSpec, Dataset as BaseDataset


DataRow = namedtuple('DataRow', 'id name data')


def slug(s, lowercase=True):
    return ''.join(
        c.lower() if lowercase else c
        for c in unicodedata.normalize('NFKD', s)
        if c.isascii() and c.isalnum())


def read_language_names(path):
    with open(path, encoding='utf-8') as f:
        rdr = csv.reader(f)
        header = next(rdr)
        name_col = header.index('Name')
        gc_col = header.index('Glottocode')
        assert name_col >= 0
        assert gc_col >= 0
        return {row[name_col]: row[gc_col] for row in rdr if row and any(row)}


def valid_language_name(language_names, name):
    if name in language_names:
        return True
    else:
        print(f'{name}: Unknown language', file=sys.stderr)
        return False


def read_csv_data(path, language_names):
    with open(path, encoding='utf-8') as f:
        rdr = csv.reader(f)
        header = next(rdr)
        name_col = header.index('Expressions')
        assert name_col > 0
        lang_cols = {
            col_no: language_names[col_name]
            for col_no, col_name in enumerate(header)
            if col_name and col_name != 'Expressions'
            if valid_language_name(language_names, col_name)}
        return [
            DataRow(
                id=row[0],
                name=row[name_col],
                data={
                    lang_cols[i]: cell
                    for i, cell in enumerate(row)
                    if i > 0 and i in lang_cols and cell and cell != '-'})
            for row in rdr
            if row and row[0]]


def normalise_csv(table):
    return [
        {k.strip(): v.strip() for k, v in row.items() if k.strip() and v.strip()}
        for row in table]


def make_languages(language_names, glottolog_langs):
    return [
        {
            'ID': language_id,
            'Name': language_name,
            'Glottocode': language_id,
            'ISO639P3code': (lg := glottolog_langs[language_id]).iso,
            'Latitude': lg.latitude,
            'Longitude': lg.longitude,
            'Macroarea': lg.macroareas[0].name if lg.macroareas else '',
        }
        for language_name, language_id in language_names.items()]


def is_example_id(id_):
    return id_.startswith('ex ')


def make_examples(datarows):
    example_rows = (
        datarow
        for datarow in datarows
        if is_example_id(datarow.id))
    return [
        {
            'ID': f'{language_id}-{slug(datarow.name)}',
            'Language_ID': language_id,
            'Primary_Text': value,
            'Translated_Text': datarow.name,
            'Parameter_ID': datarow.id.split(' ', maxsplit=1)[1],
        }
        for datarow in example_rows
        for language_id, value in datarow.data.items()]


def assoc_value_examples(examples):
    value_examples = defaultdict(list)
    for example in examples:
        value_examples[example['Language_ID'], example['Parameter_ID']].append(example['ID'])
    return value_examples


def is_parameter_id(id_):
    return id_.isnumeric()


def make_values(raw_indoaryan_data, value_examples):
    parameter_rows = (
        datarow
        for datarow in raw_indoaryan_data
        if is_parameter_id(datarow.id))
    return [
        {
            'ID': f'{datarow.id}-{language_id}',
            'Language_ID': language_id,
            'Parameter_ID': datarow.id,
            'Value': value,
            'Example_IDs': value_examples.get((language_id, datarow.id), ()),
        }
        for datarow in parameter_rows
        for language_id, value in datarow.data.items()]


def update_cldf_schema(cldf):
    cldf.add_component('LanguageTable')
    cldf.add_component('ParameterTable')
    cldf.add_component('ExampleTable')
    cldf.add_columns(
        'ValueTable',
         {
            'dc:extent': 'multivalued',
            'datatype': {
                'base': 'string',
                'format': '[a-zA-Z0-9_\\-]+',
            },
            'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#exampleReference',
            'separator': ';',
            'name': 'Example_IDs',
        })


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "mamtasouthasia"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(
            dir=self.cldf_dir,
            module="StructureDataset",
            metadata_fname='cldf-metadata.json')

    def cmd_download(self, _args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        csv_dir = self.raw_dir / 'csv-export'
        if not csv_dir.exists():
            csv_dir.mkdir()
        excel_file = self.raw_dir / 'Mamta_added.xlsx'

        def _fraction_to_str(n, enum, denom):
            if abs(n - (enum / denom)) < 0.0001:
                return f'{enum}/{denom}'
            else:
                return ''

        def _float_to_fraction(n):
            s = (
                _fraction_to_str(n, 1, 2)
                or _fraction_to_str(n, 1, 2)
                or _fraction_to_str(n, 2, 3)
                or _fraction_to_str(n, 1, 4)
                or _fraction_to_str(n, 3, 4)
                or _fraction_to_str(n, 1, 5)
                or _fraction_to_str(n, 3, 5)
                or _fraction_to_str(n, 2, 6)
                or _fraction_to_str(n, 2, 7)
                or _fraction_to_str(n, 3, 7)
                or _fraction_to_str(n, 1, 10)
                or _fraction_to_str(n, 1, 16))
            assert s, n
            return s

        def _cell_str(cell):
            # i am not fond of excel...
            if not cell.value:
                return ''
            elif isinstance(cell.value, int):
                if cell.number_format in {'General',  '# ?/?'}:
                    return str(cell.value)
                else:
                    raise AssertionError(cell.number_format)
            elif isinstance(cell.value, float):
                if cell.number_format == 'General':
                    return str(cell.value)
                elif cell.number_format == '# ?/?':
                    return _float_to_fraction(cell.value)
                elif cell.number_format == '# ??/16':
                    return f'{int(cell.value * 16)}/16'
                else:
                    raise AssertionError(cell.number_format)
            elif isinstance(cell.value, datetime.datetime):
                if cell.number_format == 'm/d':
                    return f'{cell.value.month}/{cell.value.day}'
                else:
                    raise AssertionError(cell.number_format)
            else:
                return str(cell.value.strip())

        workbook = openpyxl.load_workbook(excel_file, data_only=True)
        for sheet in workbook:
            suffix = slug(sheet.title, lowercase=False)
            csv_file = csv_dir / f'{excel_file.stem}.{suffix}.csv'
            with open(csv_file, 'w', encoding='utf-8') as f:
                wtr = csv.writer(f)
                wtr.writerows(
                    list(map(_cell_str, row))
                    for row in sheet.rows)

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """

        # read csv

        csv_dir = self.raw_dir / 'csv-export'
        language_names = read_language_names(csv_dir / 'Mamta_added.Languages.csv')
        raw_indoaryan_data = read_csv_data(
            csv_dir / 'Mamta_added.IndoAryan.csv', language_names)
        parameter_table = normalise_csv(self.etc_dir.read_csv(
            'parameters.csv', dicts=True))

        # create cldf

        glottolog_langs = {
            lg.id: lg
            for lg in args.glottolog.api.languoids(ids=set(language_names.values()))}
        language_table = make_languages(language_names, glottolog_langs)

        example_table = make_examples(raw_indoaryan_data)

        value_examples = assoc_value_examples(example_table)
        value_table = make_values(raw_indoaryan_data, value_examples)

        # write dataset

        update_cldf_schema(args.writer.cldf)

        args.writer.objects['LanguageTable'] = language_table
        args.writer.objects['ParameterTable'] = parameter_table
        args.writer.objects['ValueTable'] = value_table
        args.writer.objects['ExampleTable'] = example_table
