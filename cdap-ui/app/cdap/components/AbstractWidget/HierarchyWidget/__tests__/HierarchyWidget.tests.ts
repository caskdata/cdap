/*Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import { typeHandler, flatToTree, addPathToFlatMap, toJsonHandler } from '../dataHandler';

describe('Unit tests for data handler helper function in Hierarchy widget', () => {
  it('Should handle boolean type', () => {
    const field = {
      name: 'Id',
      type: 'boolean',
    };
    const output = {
      name: 'Id',
      type: 'boolean',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle bytes type', () => {
    const field = {
      name: 'Id',
      type: 'bytes',
    };
    const output = {
      name: 'Id',
      type: 'bytes',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle double type', () => {
    const field = {
      name: 'Id',
      type: 'double',
    };
    const output = {
      name: 'Id',
      type: 'double',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle float type', () => {
    const field = {
      name: 'Id',
      type: 'float',
    };
    const output = {
      name: 'Id',
      type: 'float',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle int type', () => {
    const field = {
      name: 'Id',
      type: 'int',
    };
    const output = {
      name: 'Id',
      type: 'int',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle long type', () => {
    const field = {
      name: 'Id',
      type: 'long',
    };
    const output = {
      name: 'Id',
      type: 'long',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle string type', () => {
    const field = {
      name: 'Id',
      type: 'string',
    };
    const output = {
      name: 'Id',
      type: 'string',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle array type', () => {
    const field = {
      name: 'Id',
      type: {
        type: 'array',
        items: 'string',
      },
      // children: ['string'],
    };
    const output = {
      name: 'Id',
      type: 'array',
      children: ['string'],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle enum type', () => {
    const field = {
      name: 'Id',
      type: {
        type: 'enum',
        symbols: ['Test'],
      },
    };
    const output = {
      name: 'Id',
      type: 'enum',
      children: ['Test'],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle map type', () => {
    const field = {
      name: 'Id',
      type: {
        type: 'map',
        keys: 'string',
        values: 'string',
      },
    };
    const output = {
      name: 'Id',
      type: 'map',
      children: [
        { name: 'keys', type: 'string' },
        { name: 'values', type: 'string' },
      ],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle record type', () => {
    const field = {
      name: 'Customer',
      type: {
        type: 'record',
        name: 'Customer',
        fields: [
          {
            name: 'Name',
            type: 'string',
          },
        ],
      },
    };
    const output = {
      name: 'Customer',
      type: 'record',
      children: [{ name: 'Name', type: 'string' }],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle union type', () => {
    const field = {
      name: 'Id',
      type: ['int', 'float', 'double'],
    };
    const output = {
      name: 'Id',
      type: 'int',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle time type', () => {
    const field = {
      name: 'Id',
      type: {
        type: 'long',
        logicalType: 'time-micros',
      },
    };
    const output = {
      name: 'Id',
      type: 'time-micros',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle timestamp type', () => {
    const field = {
      name: 'Id',
      type: {
        type: 'long',
        logicalType: 'timestamp-micros',
      },
    };
    const output = {
      name: 'Id',
      type: 'timestamp-micros',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle decimal type', () => {
    const field = {
      name: 'Id',
      type: {
        type: 'bytes',
        logicalType: 'decimal',
        precision: 32,
        scale: 3,
      },
    };
    const output = {
      name: 'Id',
      type: 'decimal',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle date type', () => {
    const field = {
      name: 'Id',
      type: {
        type: 'int',
        logicalType: 'date',
      },
    };
    const output = {
      name: 'Id',
      type: 'date',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should handle datetime type', () => {
    const field = {
      name: 'Id',
      type: {
        type: 'string',
        logicalType: 'datetime',
      },
    };
    const output = {
      name: 'Id',
      type: 'datetime',
      children: [],
    };
    const result = typeHandler(field);
    expect(result).toEqual(output);
  });

  it('Should convert flat schema to tree', () => {
    const flat = [
      { id: 1, parentId: null, name: 'Customer_orders', type: 'record' },
      { id: 2, parentId: 1, name: 'Customer', type: 'record', path: ['Customer'] },
      { id: 3, parentId: 2, name: 'Id', type: 'int', path: ['Id'] },
      { id: 4, parentId: 2, name: 'Name', type: 'string', path: ['Name'] },
      { id: 5, parentId: 2, name: 'Phone', type: 'array', path: ['Phone'] },
      { id: 6, parentId: 1, name: 'Orders', type: 'record' },
      { id: 7, parentId: 6, name: 'Order_Id', type: 'double', path: ['Order_Id'] },
      { id: 8, parentId: 6, name: 'Order_Item', type: 'record' },
      { id: 9, parentId: 8, name: 'Product_Qty', type: 'float', path: ['Product_Qty'] },
      { id: 10, parentId: 8, name: 'Product_Price', type: 'decimal', path: ['Product_Price'] },
      {
        id: 11,
        parentId: 8,
        name: 'Product_Expire_Date',
        type: 'date',
        path: ['Product_Expire_Date'],
      },
      { id: 12, parentId: 6, name: 'Payment_Method', type: 'union', path: ['Payment_Method'] },
      { id: 13, parentId: 6, name: 'Payment_Time', type: 'time', path: ['Payment_Time'] },
    ];

    const tree = [
      {
        id: 1,
        parentId: null,
        name: 'Customer_orders',
        type: 'record',
        children: [
          {
            id: 2,
            parentId: 1,
            name: 'Customer',
            type: 'record',
            path: ['Customer'],
            children: [
              {
                id: 3,
                parentId: 2,
                name: 'Id',
                type: 'int',
                path: ['Id'],
                children: [],
              },
              {
                id: 4,
                parentId: 2,
                name: 'Name',
                type: 'string',
                path: ['Name'],
                children: [],
              },
              {
                id: 5,
                parentId: 2,
                name: 'Phone',
                type: 'array',
                path: ['Phone'],
                children: [],
              },
            ],
          },
          {
            id: 6,
            parentId: 1,
            name: 'Orders',
            type: 'record',
            children: [
              {
                id: 7,
                parentId: 6,
                name: 'Order_Id',
                type: 'double',
                path: ['Order_Id'],
                children: [],
              },
              {
                id: 8,
                parentId: 6,
                name: 'Order_Item',
                type: 'record',
                children: [
                  {
                    id: 9,
                    parentId: 8,
                    name: 'Product_Qty',
                    type: 'float',
                    path: ['Product_Qty'],
                    children: [],
                  },
                  {
                    id: 10,
                    parentId: 8,
                    name: 'Product_Price',
                    type: 'decimal',
                    path: ['Product_Price'],
                    children: [],
                  },
                  {
                    id: 11,
                    parentId: 8,
                    name: 'Product_Expire_Date',
                    type: 'date',
                    path: ['Product_Expire_Date'],
                    children: [],
                  },
                ],
              },
              {
                id: 12,
                parentId: 6,
                name: 'Payment_Method',
                type: 'union',
                path: ['Payment_Method'],
                children: [],
              },
              {
                id: 13,
                parentId: 6,
                name: 'Payment_Time',
                type: 'time',
                path: ['Payment_Time'],
                children: [],
              },
            ],
          },
        ],
      },
    ];
    const result = flatToTree(flat);
    expect(result).toEqual(tree);
  });

  it('Should add path to flat map', () => {
    const flat = [
      {
        id: 1,
        name: 'Customer',
        parentId: null,
        parentIdsArr: [],
        type: 'record',
      },
      {
        id: 2,
        name: 'Name',
        parentId: 1,
        parentIdsArr: [],
        type: 'string',
      },
    ];

    const output = [
      {
        id: 1,
        name: 'Customer',
        parentId: null,
        parentIdsArr: [],
        type: 'record',
        path: ['Customer'],
      },
      {
        id: 2,
        name: 'Name',
        parentId: 1,
        parentIdsArr: [],
        type: 'string',
        path: ['Customer', 'Name'],
      },
    ];

    const result = addPathToFlatMap(flat);
    expect(result).toEqual(output);
  });

  it('Should convert the flat schema to JSON', () => {
    const flat = [
      { id: 1, parentId: null, type: 'record', children: [], name: 'Customer_orders' },
      { id: 2, parentId: 1, type: 'record', name: 'Customer', children: [] },
      { id: 3, path: ['Id'], parentId: 2, name: 'Id', children: [] },
      { id: 4, path: ['Name'], parentId: 2, name: 'Name', children: [] },
      { id: 5, path: ['Phone'], parentId: 2, name: 'Phone', children: [] },
      { id: 6, parentId: 1, type: 'record', name: 'Orders', children: [] },
      { id: 7, path: ['Order_Id'], parentId: 6, name: 'Order_Id', children: [] },
      { id: 8, parentId: 6, type: 'record', name: 'Order_Item', children: [] },
      { id: 9, path: ['Product_Qty'], parentId: 8, name: 'Product_Qty', children: [] },
      { id: 10, path: ['Product_Price'], parentId: 8, name: 'Product_Price', children: [] },
      {
        id: 11,
        path: ['Product_Expire_Date'],
        parentId: 8,
        name: 'Product_Expire_Date',
        children: [],
      },
      { id: 12, path: ['Payment_Method'], parentId: 6, name: 'Payment_Method', children: [] },
      { id: 13, path: ['Payment_Time'], parentId: 6, name: 'Payment_Time', children: [] },
    ];

    const json = {
      Customer_orders: {
        Customer: {
          Id: ['Id'],
          Name: ['Name'],
          Phone: ['Phone'],
        },
        Orders: {
          Order_Id: ['Order_Id'],
          Order_Item: {
            Product_Qty: ['Product_Qty'],
            Product_Price: ['Product_Price'],
            Product_Expire_Date: ['Product_Expire_Date'],
          },
          Payment_Method: ['Payment_Method'],
          Payment_Time: ['Payment_Time'],
        },
      },
    };

    const result = toJsonHandler(flat);
    expect(result).toEqual(json);
  });
});
