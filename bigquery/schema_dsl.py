__author__ = 'ekampf'


class FieldTypes(object):
    integer = 'INTEGER'
    float = 'FLOAT'
    string = 'STRING'
    record = 'RECORD'
    ts = 'TIMESTAMP'


class FieldMode(object):
    nullable = 'NULLABLE'
    required = 'REQUIRED'
    repeated = 'REPEATED'


def Field(name, column_type, description=None, mode=None, fields=None):
    field = dict(name=name, type=column_type)
    if description:
        field['description'] = description
    if mode:
        field['mode'] = mode
    if fields:
        field['fields'] = fields

    return field


def StringField(name, mode=None, description=None):
    return Field(name, FieldTypes.string, mode=mode, description=description)


def FloatField(name, mode=None, description=None):
    return Field(name, FieldTypes.float, mode=mode, description=description)


def IntField(name, mode=None, description=None):
    return Field(name, FieldTypes.integer, mode=mode, description=description)


def TSField(name, mode=None, description=None):
    return Field(name, FieldTypes.ts, mode=mode, description=description)


def RecordField(name, fields, mode=None, description=None):
    return Field(name, FieldTypes.record, fields=fields, description=description, mode=mode)
