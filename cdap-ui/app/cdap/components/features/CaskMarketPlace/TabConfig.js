/*
 * Copyright © 2016 Cask Data, Inc.
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

import React from 'react';

import AllTabContents from './AllTabContents';

const TabConfig = {
  defaultTab: 1,
  layout: 'vertical',
  tabs: [
    {
      id: 1,
      name: 'All',
      content: <AllTabContents />
    },
    {
      id: 2,
      name: 'Examples',
      content: <AllTabContents />
    },
    {
      id: 3,
      name: 'Use Cases',
      content: <AllTabContents />
    },
    {
      id: 4,
      name: 'Pipeline',
      content: <AllTabContents />
    },
    {
      id: 5,
      name: 'Applications',
      content: <AllTabContents />
    },
    {
      id: 6,
      name: 'Datasets',
      content: <AllTabContents />
    },
    {
      id: 7,
      name: 'Plugins',
      content: <AllTabContents />
    },
    {
      id: 8,
      name: 'Dashboards',
      content: <AllTabContents />
    },
    {
      id: 9,
      name: 'Artifacts',
      content: <AllTabContents />
    }
  ]
};

export default TabConfig;
