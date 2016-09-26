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
import StreamCreateWizard from 'components/CaskWizards/StreamCreate';
import UploadDataWizard from 'components/CaskWizards/UploadData';
import PublishPipelineWizard from 'components/CaskWizards/PublishPipeline';
import InformationalWizard from 'components/CaskWizards/Informational';
import CreateStreamStore from 'services/WizardStores/CreateStream/CreateStreamStore';
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';
import PublishPipelineStore from 'services/WizardStores/PublishPipeline/PublishPipelineStore';
import AddNamespaceWizard from 'components/CaskWizards/AddNamespace';
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';

const WizardTypesMap = {
  'create_stream': {
    tag: StreamCreateWizard,
    store: CreateStreamStore
  },
  'load_datapack': {
    tag: UploadDataWizard,
    store: UploadDataStore
  },
  'create_app': {
    tag: PublishPipelineWizard,
    store: PublishPipelineStore
  },
  'create_pipeline': {
    tag: PublishPipelineWizard,
    store: PublishPipelineStore
  },
  'create_pipeline_draft': {
    tag: PublishPipelineWizard,
    store: PublishPipelineStore
  },
  'informational': {
    tag: InformationalWizard,
    store: {}
  },
  'add_namespace': {
    tag: AddNamespaceWizard,
    store: AddNamespaceStore
  }
};

export default function AbstractWizard({isOpen, onClose, wizardType, input}) {
  if (!isOpen) {
    return null;
  }
  let {tag: Tag, store} = WizardTypesMap[wizardType];
  if (!Tag) {
    return (<h1> Wizard Type {wizardType} not found </h1>);
  }
  return (
    React.createElement(Tag, {
      isOpen,
      onClose,
      store,
      input
    })
  );
}
AbstractWizard.propTypes = {
  isOpen: PropTypes.bool,
  wizardType: PropTypes.string,
  onClose: PropTypes.func,
  input: PropTypes.any
};
