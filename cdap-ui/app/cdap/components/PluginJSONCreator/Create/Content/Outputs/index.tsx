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

import Heading, { HeadingTypes } from 'components/Heading';
import JsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import {
  CreateContext,
  createContextConnect,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const OutputsView: React.FC<ICreateContext> = ({
  pluginName,
  pluginType,
  displayName,
  emitAlerts,
  emitErrors,
  configurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetToInfo,
  widgetToAttributes,
  jsonView,
  setJsonView,
  outputName,
  setOutputName,
  setPluginState,
}) => {
  const [localOutputName, setLocalOutputName] = React.useState(outputName);

  // In case user uploads new file
  React.useEffect(() => {
    setLocalOutputName(outputName);
  }, [outputName]);

  function saveAllResults() {
    setOutputName(localOutputName);
  }

  return (
    <div>
      <JsonMenu
        pluginName={pluginName}
        pluginType={pluginType}
        displayName={displayName}
        emitAlerts={emitAlerts}
        emitErrors={emitErrors}
        configurationGroups={configurationGroups}
        groupToInfo={groupToInfo}
        groupToWidgets={groupToWidgets}
        widgetToInfo={widgetToInfo}
        widgetToAttributes={widgetToAttributes}
        jsonView={jsonView}
        setJsonView={setJsonView}
        outputName={localOutputName}
        setPluginState={setPluginState}
      />
      <Heading type={HeadingTypes.h3} label="Outputs" />
      <br />
      <PluginInput
        widgetType={'textbox'}
        value={localOutputName}
        setValue={setLocalOutputName}
        label={'Output Name'}
        placeholder={'output name'}
        required={false}
      />
      <StepButtons nextDisabled={false} onPrevious={saveAllResults} onNext={saveAllResults} />
    </div>
  );
};

const Outputs = createContextConnect(CreateContext, OutputsView);
export default Outputs;
