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
import React, { Component, PropTypes } from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';
import UploadDataWizardConfig from 'services/WizardConfigs/UploadDataWizardConfig';
import UploadDataActions from 'services/WizardStores/UploadData/UploadDataActions';
import UploadDataActionCreator from 'services/WizardStores/UploadData/ActionCreator';

import head from 'lodash/head';

require('./UploadData.less');

export default class UploadDataWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };
    this.prepareInputForSteps();
  }
  prepareInputForSteps() {
    let action = this.props.input.action;
    let filename = head(action.arguments.filter(arg => arg.name === 'files'));

    if (filename && filename.value.length) {
      filename = filename.value[0];
    }
    UploadDataStore.dispatch({
      type: UploadDataActions.setFilename,
      payload: { filename }
    });
    UploadDataStore.dispatch({
      type: UploadDataActions.setPackageInfo,
      payload: {
        name: this.props.input.package.name,
        version: this.props.input.package.version
      }
    });
  }
  onSubmit() {
    let state = UploadDataStore.getState();
    let streamId = state.selectdestination.name;
    let filename = state.viewdata.filename;
    let filetype = 'text/' + filename.split('.').pop();
    let fileContents = state.viewdata.data;
    return UploadDataActionCreator.uploadData({
      url: '/namespaces/default/streams/' + streamId + '/batch',
      fileContents,
      headers: {
        filetype,
        filename
      }
    });
  }
  toggleWizard(returnResult) {
    if (this.state.showWizard) {
      this.props.onClose(returnResult);
    }
    this.setState({
      showWizard: !this.state.showWizard
    });
  }
  componentWillUnmount() {
    UploadDataStore.dispatch({
      type: UploadDataActions.onReset
    });
  }
  render() {
    return (
      <WizardModal
        title={this.props.input.label ? this.props.input.label + " | Upload Data" : "Upload Data"}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="upload-data-wizard"
      >
        <Wizard
          wizardConfig={UploadDataWizardConfig}
          store={UploadDataStore}
          onSubmit={this.onSubmit.bind(this)}
          onClose={this.toggleWizard.bind(this)}/>
      </WizardModal>
    );
  }
}
UploadDataWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
