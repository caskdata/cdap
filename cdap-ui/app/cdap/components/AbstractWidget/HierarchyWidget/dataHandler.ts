/*
 * Copyright © 2021 Cask Data, Inc.
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

import { IStageSchema } from 'components/AbstractWidget';
import uuidV4 from 'uuid/v4';

interface IFieldType {
  name: string;
  type: string[] | string | IFieldType;
}

function enumHandler(field) {
  return { name: field.name, type: field.type.type, children: field.type.symbols };
}

function mapHandler(field) {
  const objectKeys = Object.keys(field.type);
  const objectValues = Object.values(field.type);
  return {
    name: field.name,
    type: field.type.type,
    children: [
      { name: objectKeys[1], type: objectValues[1] },
      { name: objectKeys[2], type: objectValues[2] },
    ],
  };
}

function intByteHandler(field) {
  return { name: field.name, type: field.type.logicalType, children: [] };
}

function longHandler(field) {
  return { name: field.name, type: field.type.logicalType, children: [] };
}

function dateTimeHandler(field) {
  return { name: field.name, type: field.type.logicalType, children: [] };
}

function recordHandler(field) {
  return {
    name: field.name,
    type: field.type.type,
    children: field.type.fields,
  };
}

function arrayHandler(field) {
  return { name: field.name, type: field.type.type, children: [field.type.items] };
}

function stringHandler(field) {
  return { name: field.name, type: field.type, children: [] };
}

interface IField {
  name: string;
  type: string | IField;
}

// The function receives a field from inputschema and is meant to destructure and
// return it in the format that the Autocomplete component is expecting
function getType(field: IField) {
  let fieldFormat = {};

  switch (typeof field.type) {
    case 'object':
      switch (field.type.type) {
        case 'enum':
          fieldFormat = enumHandler(field);
          break;
        case 'map':
          fieldFormat = mapHandler(field);
          break;
        case 'bytes':
        case 'int':
          fieldFormat = intByteHandler(field);
          break;
        case 'long':
          fieldFormat = longHandler(field);
          break;
        case 'record':
          fieldFormat = recordHandler(field);
          break;
        case 'array':
          fieldFormat = arrayHandler(field);
          break;
        case 'string':
          fieldFormat = dateTimeHandler(field);
          break;
        default:
          break;
      }
      return fieldFormat;
    case 'string':
      fieldFormat = stringHandler(field);
      break;
    default:
      break;
  }
  return fieldFormat;
}

// This function receives a field from the input schema and depending on the condition returns the expected format
export function typeHandler(field) {
  let fieldFormat = {};
  if (Array.isArray(field.type)) {
    const objToFormat = {
      name: field.name,
      type: field.type[0],
    };
    fieldFormat = getType(objToFormat);
    typeHandler(objToFormat);
  } else {
    fieldFormat = getType(field);
  }
  return fieldFormat;
}

export function getFields(schemas: IStageSchema[], allowedTypes: string[]) {
  let fields = [];
  if (!schemas || schemas.length === 0) {
    return fields;
  }
  const stage = schemas[0];

  try {
    const unparsedFields = JSON.parse(stage.schema).fields;

    if (unparsedFields.length > 0) {
      fields = unparsedFields
        .filter((field: IFieldType) => containsType(field.type, allowedTypes))
        .map((field: IFieldType) => {
          const fieldFormat = typeHandler(field);
          return fieldFormat;
        });
    }
  } catch {
    // tslint:disable-next-line: no-console
    console.log('Error: Invalid JSON schema');
  }
  return fields;
}

function containsType(types: string[] | IFieldType | string, allowedTypes: string[]) {
  if (allowedTypes.length === 0) {
    return true;
  }

  return allowedTypes.includes(extractType(types));
}

function extractType(types) {
  let value = types;
  if (types instanceof Array) {
    if (types.length === 1) {
      value = types[0];
    } else if (types.length === 2 && types.includes('null')) {
      value = types.indexOf('null') === 0 ? types[1] : types[0];
    }
  }

  if (typeof value === 'object') {
    value = value.logicalType || value;
  }
  return value;
}

interface IFlatRow {
  id: number;
  parentId?: number;
  name: string;
  path?: string[];
  type: string;
  children?: IFlatRow[];
}

// This function receives an array of flat rows, and converts it to a tree (array)
export const flatToTree = (items: IFlatRow[]) => {
  const rootItems = [];
  const lookup = {};

  for (const item of items) {
    const itemId = item.id;
    const parentId = item.parentId;

    if (!lookup[itemId]) {
      lookup[itemId] = { ['children']: [] };
    }
    lookup[itemId] = { ...item, ['children']: lookup[itemId].children };

    const TreeItem = lookup[itemId];

    if (parentId === null || parentId === undefined) {
      rootItems.push(TreeItem);
    } else {
      if (!lookup[parentId]) {
        lookup[parentId] = { ['children']: [] };
      }

      lookup[parentId].children.push(TreeItem);
    }
  }
  return rootItems;
};

// This function receives the initial path, parentId of an element, the data where to loop,
// and calculates the path based on the parentId received
function findPath(path, parentId, data) {
  data.forEach((temp) => {
    if (temp.id === parentId) {
      path.push(temp.name);
      if (temp.parentId != null) {
        findPath(path, temp.parentId, data);
      }
    }
  });
  return path;
}

// The function receives an array of flat rows and adds the path property to every row
export function addPathToFlatMap(data) {
  data.forEach((tmp) => {
    if (!tmp.path) {
      if (tmp.parentId != null) {
        if (!tmp.path) {
          tmp.path = [tmp.name];
        }
        tmp.path = findPath(tmp.path, tmp.parentId, data).reverse();
      } else {
        tmp.path = [tmp.name];
      }
    }
  });
  return data;
}

// The function receives a flat schema and is meant to convert it into the
// correct JSON format that the backend is expecting
export function toJsonHandler(records) {
  const data = [...records];
  const json = {};
  data.forEach((d) => {
    if (d.parentId === null) {
      json[d.name] = {};
      json[d.name] = jsonRec(d, data, json[d.name]);
    }
  });
  return json;
}

// A recursive function that receives a parent element, a flat schema and the property in
// which is meant to add children or assign a value depending on its type
function jsonRec(d, data, json) {
  data.map((k) => {
    if (k.parentId === d.id) {
      json[k.name] = {};
      if (k.type === 'record' && !k.path) {
        jsonRec(k, data, json[k.name]);
        return;
      }
      json[k.name] = k.path;
    }
  });
  return json;
}

// The function receives json format and is meant to convert it into the
// correct flat schema format that the frontend will use to render the records
export function jsonToFlat(json) {
  let result = [];
  const keys = Object.keys(json);
  keys.map((key) => {
    const uniqueId = uuidV4();

    if (typeof json[key] === 'object') {
      result.push({ id: uniqueId, name: key, parentId: null, type: 'record' });
      result = iterateChildren(uniqueId, json[key], result);
    }
  });
  return result;
}

// This function receives a parentId, and object and an array,
// it iterates through the keys of the objects and returns a flat row
function iterateChildren(parentId, tree, result) {
  const keys = Object.keys(tree);

  keys.map((key: string) => {
    const uniqueId = uuidV4();
    if (!Array.isArray(tree[key])) {
      result.push({ id: uniqueId, name: key, parentId, type: 'record' });
      iterateChildren(uniqueId, tree[key], result);
      return;
    }
    result.push({ id: uniqueId, name: key, parentId, type: 'string', path: tree[key] });
  });
  return result;
}

// This function receives two arrays, one to push and one to loop, a unique id and a margin number,
// it returns an array with flat rows
export const getChildren = (childrenOfSelected, item, uniqueId, arrPrId) => {
  const arrOfParentIds = arrPrId;
  arrOfParentIds.push(uniqueId);
  item.forEach((child) => {
    const childId = uuidV4();
    let typeOfChild: any = {};
    typeOfChild = typeHandler(child);
    childrenOfSelected.push({
      path: child.path,
      id: childId,
      parentId: uniqueId,
      name: child.name ? child.name : child,
      type: typeOfChild.type,
      children: [],
      parentIdsArr: arrOfParentIds,
    });

    if (typeof child.type === 'object') {
      getChildren(childrenOfSelected, typeOfChild.children, childId, arrOfParentIds);
    }
  });
  return childrenOfSelected;
};

// This function receives the input schema fields and converts it into an array of flat rows which the UI expects
export const inputSchemaToFlat = (fieldValues) => {
  let dropdownData = [];
  fieldValues.forEach((item) => {
    let childrenOfSelected = [];
    const uniqueId = uuidV4();

    if (item.children.length !== 0) {
      const arrOfParentIds = [];
      childrenOfSelected = getChildren(childrenOfSelected, item.children, uniqueId, arrOfParentIds);
    }
    const childToBeAdded = {
      id: uniqueId,
      parentId: null,
      name: item.name,
      children: [],
      type: item.type,
      parentIdsArr: [],
    };
    dropdownData.push(childToBeAdded);
    if (childrenOfSelected.length !== 0) {
      childrenOfSelected.forEach((child) => {
        dropdownData.push(child);
      });
    }
  });
  dropdownData = addPathToFlatMap(dropdownData);
  return dropdownData;
};

// 1. (base) There is no node so there is nothing left to process; return the result r
// 2. (induction) There is at least one node. If the node's id or parentid is in the set s, a matching node has been found.
// Add the node's id to the set and start the search over with the partial result r and the remaining nodes, more.
// 3. (induction) There is at least one node and it does not match the ids we are searching for.
// Append the node to the result and continue searching more nodes.

const recur = (...values) => ({ recur, values });

const iterateNode = (fa) => {
  let a = fa();
  while (a && a.recur === recur) {
    a = fa(...a.values);
  }
  return a;
};

// This function repeatedly searches for children of nodes known to be eliminated until no new such children can be found anymore.
// Therefore it keeps track of currently known descendants in a dictionary
export const removeFamily = (id = 0, nodes = []) =>
  iterateNode(([node, ...more] = nodes, s = new Set([id]), r = []) => {
    if (node === undefined) {
      return r;
    }
    if (s.has(node.id) || s.has(node.parentId)) {
      return recur([...r, ...more], s.add(node.id), []);
    }
    return recur(more, s, [...r, node]);
  });
