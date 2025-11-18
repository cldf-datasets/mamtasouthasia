import csv
import datetime
import pathlib
import sys
import unicodedata
from collections import namedtuple, defaultdict
from itertools import chain, islice

import openpyxl

from cldfbench import CLDFSpec, Dataset as BaseDataset


def slug(s, lowercase=True):
    return ''.join(
        c.lower() if lowercase else c
        for c in unicodedata.normalize('NFKD', s)
        if c.isascii() and c.isalnum())


def normalise_whitespace(s):
    return ' '.join(s.split()).strip()


def normalise_csv(table):
    return [
        {k.strip(): v.strip() for k, v in row.items() if k.strip() and v.strip()}
        for row in table]


def read_language_names(path):
    with open(path, encoding='utf-8') as f:
        rdr = csv.reader(f)
        header = next(rdr)
        name_col = header.index('Name')
        gc_col = header.index('Glottocode')
        assert name_col >= 0
        assert gc_col >= 0
        return {row[name_col]: row[gc_col] for row in rdr if row and any(row)}


def lookup_language(language_names, name, sheet_name):
    if (language_id := language_names.get(name)):
        return language_id
    elif name:
        print(f'{sheet_name}: Unknown language: {name}', file=sys.stderr)
        return None
    else:
        return None


ParameterRow = namedtuple('ParameterRow', 'id name')
ExampleRow = namedtuple('ExampleRow', 'param_id translation')

TYPO_MAP = {
    'twelvth': 'twelfth',
    'twentyth': 'twentieth',
    'forthman': 'fourthman',
    'forthwoman': 'fourthwoman',
    'iate116ofthrpizza': 'iate116ofthepizza',
    'thrice': 'thricethreetimes',
    'forthtime': 'fourtimes',
    'imethimthrice': 'imethimthreetimes',
    'imethimforthtime': 'imethimfourtimes',
    'twopairofsocks': 'twopairsofsocks',
    'pairofsocksarelyinghere': 'apairofsocksarelyinghere',
}


def fold_name(name):
    return TYPO_MAP.get(slug(name), slug(name))


def get_parameter_names(data):
    id_col = 0
    header = data[0]
    name_col = header.index('Expressions')
    assert(name_col > 0)
    return {
        fold_name(row[name_col]): ParameterRow(
            id=id_,
            name=normalise_whitespace(row[name_col]))
        for row in islice(data, 1, None)
        if (id_ := row[id_col].strip()).isnumeric()}


def get_example_names(data):
    id_col = 0
    header = data[0]
    name_col = header.index('Expressions')
    assert(name_col > 0)
    return {
        fold_name(row[name_col]): ExampleRow(
            param_id=param_id,
            translation=normalise_whitespace(row[name_col]))
        for row in islice(data, 1, None)
        if (id_ := row[id_col].strip()).startswith('ex ')
        and (param_id := id_.split(' ', maxsplit=1)[1])}


def validate_sheet(data, parameter_names, example_names):
    header = data[0]
    name_col = header.index('Expressions')
    section_headers = {
        'Cardinals (Values)',
        'Cardinals (Examples)',
        'Ordinals (Values)',
        'Ordinals (Examples)',
        'Fractions (Values)',
        'Fractions (Examples)',
        'Multiplicatives (Values)',
        'Multiplicatives (Examples)',
        'Aggregatives (Values)',
        'Aggregatives (Examples)',
        'Approximatives (Values)',
        'Approximatives (Examples)',
        'Collectives (Values)',
        'Collectives (Examples)',
        'Distributives (Values)',
        'Multipliatives (Examples)',
        'Restrictives (Values)',
        'Restrictives (Examples)',
        'Restrictive (Examples)',
    }

    def _is_good(name):
        return (
            fold_name(name) in parameter_names
            or fold_name(name) in example_names
            or name in section_headers)

    errors = [
        name
        for row in islice(data, 1, None)
        if (name := normalise_whitespace(row[name_col]))
        and not _is_good(name)]
    assert not errors, '\n'.join(errors)


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


def make_examples(data, example_names, language_names, sheet_name):
    header = data[0]
    name_col = header.index('Expressions')
    lg_cols = [
        (i, language_id)
        for i, name in islice(enumerate(header), 1, None)
        if i != name_col
        and (language_id := lookup_language(language_names, name, sheet_name))]

    example_rows = (
        (row, ex_row)
        for row in islice(data, 1, None)
        if (ex_row := example_names.get(fold_name(row[name_col]))))
    return [
        {
            'ID': f'{language_id}-{slug(row[name_col])}',
            'Language_ID': language_id,
            'Primary_Text': primary,
            'Translated_Text': ex_row.translation,
            'Parameter_ID': ex_row.param_id,
        }
        for row, ex_row in example_rows
        for i, language_id in lg_cols
        if (primary := normalise_whitespace(row[i])) and primary != '-']


def assoc_value_examples(examples):
    value_examples = defaultdict(list)
    for example in examples:
        value_examples[example['Language_ID'], example['Parameter_ID']].append(example['ID'])
    return value_examples


def make_values(data, parameter_names, language_names, value_examples):
    header = data[0]
    name_col = header.index('Expressions')
    lg_cols = [
        (i, language_id)
        for i, name in islice(enumerate(header), 1, None)
        if i != name_col
        and (language_id := language_names.get(name))]

    parameter_rows = (
        (row, param_row)
        for row in data
        if (param_row := parameter_names.get(fold_name(row[name_col]))))
    return [
        {
            'ID': f'{param_row.id}-{language_id}',
            'Language_ID': language_id,
            'Parameter_ID': param_row.id,
            'Value': value,
            'Example_IDs': value_examples.get((language_id, param_row.id), ()),
        }
        for row, param_row in parameter_rows
        for i, language_id in lg_cols
        if (value := normalise_whitespace(row[i])) and value != '-']


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

        def _cell_str(cell):  # noqa: PLR0911
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
        parameter_table = normalise_csv(self.etc_dir.read_csv(
            'parameters.csv', dicts=True))

        data_indoaryan = list(csv_dir.read_csv('Mamta_added.IndoAryan.csv'))
        data_sinotibetan = list(csv_dir.read_csv('Mamta_added.SinoTibetan.csv'))
        data_kiranti = list(csv_dir.read_csv('Mamta_added.SinoTibetanKiranti.csv'))
        data_austroasiatic = list(csv_dir.read_csv('Mamta_added.AustroAsiatic.csv'))
        data_dravidian = list(csv_dir.read_csv('Mamta_added.Dravidian.csv'))
        data_taikadai = list(csv_dir.read_csv('Mamta_added.TaiKadai.csv'))

        # create cldf

        glottolog_langs = {
            lg.id: lg
            for lg in args.glottolog.api.languoids(ids=set(language_names.values()))}
        language_table = make_languages(language_names, glottolog_langs)

        # the Indo-Aryan table contains all the ids
        parameter_names = get_parameter_names(data_indoaryan)
        example_names = get_example_names(data_indoaryan)

        # just to double-check
        validate_sheet(data_indoaryan, parameter_names, example_names)
        validate_sheet(data_sinotibetan, parameter_names, example_names)
        validate_sheet(data_kiranti, parameter_names, example_names)
        validate_sheet(data_austroasiatic, parameter_names, example_names)
        validate_sheet(data_dravidian, parameter_names, example_names)
        validate_sheet(data_taikadai, parameter_names, example_names)

        example_table = list(chain(
            make_examples(data_indoaryan, example_names, language_names, 'Indo-Aryan'),
            make_examples(data_sinotibetan, example_names, language_names, 'Sino-Tibetan'),
            make_examples(data_kiranti, example_names, language_names, 'Sino-Tibetan-Kiranti'),
            make_examples(data_austroasiatic, example_names, language_names, 'Austro-Asiatic'),
            make_examples(data_dravidian, example_names, language_names, 'Dravidian'),
            make_examples(data_taikadai, example_names, language_names, 'Tai-Kadai'),
        ))

        value_examples = assoc_value_examples(example_table)
        value_table = list(chain(
            make_values(data_indoaryan, parameter_names, language_names, value_examples),
            make_values(data_sinotibetan, parameter_names, language_names, value_examples),
            make_values(data_kiranti, parameter_names, language_names, value_examples),
            make_values(data_austroasiatic, parameter_names, language_names, value_examples),
            make_values(data_dravidian, parameter_names, language_names, value_examples),
            make_values(data_taikadai, parameter_names, language_names, value_examples),
        ))

        # write dataset

        update_cldf_schema(args.writer.cldf)

        args.writer.objects['LanguageTable'] = language_table
        args.writer.objects['ParameterTable'] = parameter_table
        args.writer.objects['ValueTable'] = value_table
        args.writer.objects['ExampleTable'] = example_table
