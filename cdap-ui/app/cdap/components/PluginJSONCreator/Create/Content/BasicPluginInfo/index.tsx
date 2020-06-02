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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Heading, { HeadingTypes } from 'components/Heading';
import { PluginTypes } from 'components/PluginJSONCreator/constants';
import JsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import {
  CreateContext,
  createContextConnect,
  IBasicPluginInfo,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    basicPluginInputs: {
      '& > *': {
        marginTop: '30px',
        marginBottom: '30px',
      },
    },
  };
};

const BasicPluginInfoView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  pluginName,
  pluginType,
  displayName,
  emitAlerts,
  emitErrors,
  setBasicPluginInfo,
  configurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetToInfo,
  widgetToAttributes,
  liveView,
  setLiveView,
  outputName,
  setPluginState,
  filters,
  filterToName,
  filterToCondition,
  filterToShowList,
  showToInfo,
}) => {
  const [localPluginName, setLocalPluginName] = React.useState(pluginName);
  const [localPluginType, setLocalPluginType] = React.useState(pluginType);
  const [localDisplayName, setLocalDisplayName] = React.useState(displayName);
  const [localEmitAlerts, setLocalEmitAlerts] = React.useState(emitAlerts);
  const [localEmitErrors, setLocalEmitErrors] = React.useState(emitErrors);

  // In case user uploads new file
  React.useEffect(() => {
    setLocalPluginName(pluginName);
  }, [pluginName]);
  React.useEffect(() => {
    setLocalPluginType(pluginType);
  }, [pluginType]);
  React.useEffect(() => {
    setLocalDisplayName(displayName);
  }, [displayName]);
  React.useEffect(() => {
    setLocalEmitAlerts(emitAlerts);
  }, [emitAlerts]);
  React.useEffect(() => {
    setLocalEmitErrors(emitErrors);
  }, [emitErrors]);

  const requiredFilledOut =
    localPluginName.length > 0 && localPluginType.length > 0 && localDisplayName.length > 0;

  function handleNext() {
    setBasicPluginInfo({
      pluginName: localPluginName,
      pluginType: localPluginType,
      displayName: localDisplayName,
      emitAlerts: localEmitAlerts,
      emitErrors: localEmitErrors,
    } as IBasicPluginInfo);
  }

  return (
    <div>
      <JsonMenu
        pluginName={localPluginName}
        pluginType={localPluginType}
        displayName={localDisplayName}
        emitAlerts={localEmitAlerts}
        emitErrors={localEmitErrors}
        configurationGroups={configurationGroups}
        groupToInfo={groupToInfo}
        groupToWidgets={groupToWidgets}
        widgetToInfo={widgetToInfo}
        widgetToAttributes={widgetToAttributes}
        liveView={liveView}
        setLiveView={setLiveView}
        outputName={outputName}
        setPluginState={setPluginState}
        filters={filters}
        filterToName={filterToName}
        filterToCondition={filterToCondition}
        filterToShowList={filterToShowList}
        showToInfo={showToInfo}
      />
      <Heading type={HeadingTypes.h3} label="Basic Plugin Information" />
      <div className={classes.basicPluginInputs}>
        <PluginInput
          widgetType={'textbox'}
          value={localPluginName}
          setValue={setLocalPluginName}
          label={'Plugin Name'}
          placeholder={'Select a Plugin Name'}
          required={true}
        />
        <PluginInput
          widgetType={'select'}
          value={localPluginType}
          setValue={setLocalPluginType}
          label={'Plugin Type'}
          options={PluginTypes}
          required={true}
        />
        <PluginInput
          widgetType={'textbox'}
          value={localDisplayName}
          setValue={setLocalDisplayName}
          label={'Display Name'}
          placeholder={'Select a Display Name'}
          required={true}
        />
        <PluginInput
          widgetType={'toggle'}
          value={localEmitAlerts ? 'true' : 'false'}
          setValue={setLocalEmitAlerts}
          label={'Emit Alerts?'}
        />
        <PluginInput
          widgetType={'toggle'}
          value={localEmitErrors ? 'true' : 'false'}
          setValue={setLocalEmitErrors}
          label={'Emit Errors?'}
        />
      </div>
      <StepButtons nextDisabled={!requiredFilledOut} onNext={handleNext} />
    </div>
  );
};

const StyledBasicPluginInfoView = withStyles(styles)(BasicPluginInfoView);
const BasicPluginInfo = createContextConnect(CreateContext, StyledBasicPluginInfoView);
export default BasicPluginInfo;
