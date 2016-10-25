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

import React, {PropTypes} from 'react';
import NamespaceStore from 'services/NamespaceStore';
import {MyAppApi} from 'api/app';
import {MyArtifactApi} from 'api/artifact';
import {MyDatasetApi} from 'api/dataset';
import {MyStreamApi} from 'api/stream';
import FastActionButton from '../FastActionButton';
import T from 'i18n-react';

export default function DeleteAction({entity, onSuccess}) {
  let api;
  let params = {
    namespace: NamespaceStore.getState().selectedNamespace
  };
  switch (entity.type) {
    case 'application':
      api = MyAppApi.delete;
      params.appId = entity.id;
      break;
    case 'artifact':
      api = MyArtifactApi.delete;
      params.artifactId = entity.id;
      params.version = entity.version;
      break;
    case 'datasetinstance':
      api = MyDatasetApi.delete;
      params.datasetId = entity.id;
      break;
    case 'stream':
      api = MyStreamApi.delete;
      params.streamId = entity.id;
      break;
  }

  function action() {
    let confirmation = confirm(
      T.translate('features.FastAction.deleteConfirmation', {entityId: entity.id})
    );

    if (confirmation) {
      api(params)
        .subscribe(onSuccess, (err) => {
          alert(err);
        });
    }
  }

  return (
    <FastActionButton
      icon="fa fa-trash"
      action={action}
    />
  );
}

DeleteAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    version: PropTypes.string,
    type: PropTypes.oneOf(['application', 'artifact', 'datasetinstance', 'stream']).isRequired
  }),
  onSuccess: PropTypes.func
};
