/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import {
  INode,
  IOrderedChildren,
} from 'components/AbstractWidget/SchemaEditor/Context/SchemaParser';
import {
  ISchemaType,
  IEnumFieldBase,
  IFieldType,
  IRecordField,
} from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import uuidV4 from 'uuid/v4';
import {
  getDefaultEmptyAvroSchema,
  InternalTypesEnum,
  AvroSchemaTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import { isDisplayTypeComplex } from 'components/AbstractWidget/SchemaEditor/SchemaHelpers';
import isEmpty from 'lodash/isEmpty';

function generateArrayType(children: IOrderedChildren, nullable: boolean) {
  const finalType = {
    type: AvroSchemaTypesEnum.ARRAY,
    items: null,
  };
  for (const childId of Object.keys(children)) {
    const currentChild = children[childId];
    const { type: childType, nullable: isArrayTypeNullable } = currentChild;
    const isArrayTypeComplex = isDisplayTypeComplex({ type: childType });
    if (!isArrayTypeComplex) {
      finalType.items = isArrayTypeNullable ? [childType, 'null'] : childType;
      continue;
    }
    // nested complex types.
    const complexType = generateSchemaFromComplexType(
      childType,
      currentChild,
      currentChild.nullable
    );
    if (complexType) {
      finalType.items = complexType;
    }
  }
  return nullable ? [finalType, 'null'] : finalType;
}

function generateMapType(children: IOrderedChildren, nullable) {
  const finalType = {
    type: AvroSchemaTypesEnum.MAP,
    keys: AvroSchemaTypesEnum.STRING,
    values: AvroSchemaTypesEnum.STRING,
  };
  for (const childId of Object.keys(children)) {
    const currentChild = children[childId];
    const { type, nullable: isCurrentChildNullable, internalType } = currentChild;
    const isMapChildComplexType = isDisplayTypeComplex({ type });
    if (!isMapChildComplexType) {
      if (internalType === InternalTypesEnum.MAP_KEYS_SIMPLE_TYPE) {
        finalType.keys = isCurrentChildNullable ? [type, 'null'] : type;
      }
      if (internalType === InternalTypesEnum.MAP_VALUES_SIMPLE_TYPE) {
        finalType.values = isCurrentChildNullable ? [type, 'null'] : type;
      }
      continue;
    }
    // nested complex types.
    const complexType = generateSchemaFromComplexType(type, currentChild, isCurrentChildNullable);
    if (internalType === InternalTypesEnum.MAP_KEYS_COMPLEX_TYPE_ROOT) {
      finalType.keys = complexType as any;
    }
    if (internalType === InternalTypesEnum.MAP_VALUES_COMPLEX_TYPE_ROOT) {
      finalType.values = complexType as any;
    }
  }
  return nullable ? [finalType, 'null'] : finalType;
}

function generateEnumType(children: IOrderedChildren, currentNode: INode, nullable) {
  const finalType: IEnumFieldBase = {
    type: AvroSchemaTypesEnum.ENUM,
    symbols: [],
  };
  const { typeProperties: currentTypeProperties = {} } = currentNode;
  if (currentTypeProperties.doc) {
    finalType.doc = currentTypeProperties.doc;
  }
  if (currentTypeProperties.aliases) {
    finalType.aliases = currentTypeProperties.aliases;
  }
  if (Array.isArray(children.order)) {
    for (const childId of children.order) {
      const currentChild = children[childId];
      const { typeProperties } = currentChild;
      if (typeProperties.symbol && typeProperties.symbol !== '') {
        finalType.symbols.push(typeProperties.symbol);
      }
      if (typeProperties.doc) {
        finalType.doc = typeProperties.doc;
      }
      if (typeProperties.aliases) {
        finalType.aliases = typeProperties.aliases;
      }
    }
  }
  return nullable ? [finalType, 'null'] : finalType;
}

function generateRecordType(children: IOrderedChildren, currentNode: INode, nullable: boolean) {
  const finalType: IRecordField = {
    type: AvroSchemaTypesEnum.RECORD,
    name: currentNode.name || `name_${uuidV4()}`,
    fields: [],
  };
  finalType.name = finalType.name.replace(/-/g, '');
  const { typeProperties = {} } = currentNode;
  if (typeProperties.doc) {
    finalType.doc = typeProperties.doc;
  }
  if (typeProperties.aliases) {
    finalType.aliases = typeProperties.aliases;
  }
  if (Array.isArray(children.order)) {
    for (const childId of children.order) {
      const currentChild = children[childId];
      const { name, type, nullable: isFieldNullable } = currentChild;
      if (!name || name === '') {
        continue;
      }
      const isFieldTypeComplex = isDisplayTypeComplex({ type });
      if (!isFieldTypeComplex) {
        finalType.fields.push({
          name,
          type: isFieldNullable ? [type, 'null'] : type,
        });
      } else {
        finalType.fields.push({
          name,
          type: generateSchemaFromComplexType(
            currentChild.type,
            currentChild,
            currentChild.nullable
          ),
        });
      }
    }
  }
  return nullable ? [finalType, 'null'] : finalType;
}

function generateUnionType(children: IOrderedChildren) {
  const finalType = [];
  if (Array.isArray(children.order)) {
    for (const childId of children.order) {
      const currentChild = children[childId];
      const { type } = currentChild;
      const isUnionTypeComplex = isDisplayTypeComplex({ type });
      if (!isUnionTypeComplex) {
        finalType.push(type);
        continue;
      }
      finalType.push(generateSchemaFromComplexType(type, currentChild, false));
    }
  }
  return finalType;
}

function generateLogicalType(child) {
  const { typeProperties, nullable } = child;
  return nullable ? [typeProperties, 'null'] : typeProperties;
}

function generateSchemaFromComplexType(type: string, currentChild, nullable: boolean) {
  const complexTypeChildren: IOrderedChildren = currentChild.children;
  switch (type) {
    case AvroSchemaTypesEnum.ARRAY:
      return generateArrayType(complexTypeChildren, nullable);
    case AvroSchemaTypesEnum.MAP:
      return generateMapType(complexTypeChildren, nullable);
    case AvroSchemaTypesEnum.ENUM:
      return generateEnumType(complexTypeChildren, currentChild, nullable);
    case AvroSchemaTypesEnum.UNION:
      return generateUnionType(complexTypeChildren);
    case AvroSchemaTypesEnum.RECORD:
      return generateRecordType(complexTypeChildren, currentChild, nullable);
    case AvroSchemaTypesEnum.TIME:
    case AvroSchemaTypesEnum.TIMESTAMP:
    case AvroSchemaTypesEnum.DECIMAL:
    case AvroSchemaTypesEnum.DATE:
      return generateLogicalType(currentChild);
    default:
      return type;
  }
}

/**
 * Utility to convert the entire schema tree to a valid avro schema JSON.
 * @param schemaTree Schema tree to convert to avro schema JSON.
 */
function SchemaGenerator(schemaTree: INode) {
  const avroSchema: ISchemaType = getDefaultEmptyAvroSchema();
  if (!schemaTree) {
    return avroSchema;
  } else {
    avroSchema.schema.fields = [];
  }
  avroSchema.schema.fields = [];
  avroSchema.name = schemaTree.name || avroSchema.name;
  // Top level record fields.
  const { order } = schemaTree.children;
  if (Array.isArray(order)) {
    for (const id of order) {
      const currentField = schemaTree.children[id];
      const { name, type, nullable, typeProperties } = currentField;
      // Skip the newly added rows.
      if (!name || name === '') {
        continue;
      }
      const isFieldComplexType = isDisplayTypeComplex({ type });
      const field: IFieldType = {
        name,
        type: nullable ? [type, 'null'] : type,
      };
      if (isFieldComplexType) {
        field.type = generateSchemaFromComplexType(type, currentField, nullable);
      }
      if (typeProperties && typeProperties.doc) {
        field.doc = typeProperties.doc;
      }
      if (typeProperties && typeProperties.aliases) {
        field.aliases = typeProperties.aliases.slice();
      }
      avroSchema.schema.fields.push(field);
    }
  }
  return avroSchema;
}

export { SchemaGenerator, generateSchemaFromComplexType };
