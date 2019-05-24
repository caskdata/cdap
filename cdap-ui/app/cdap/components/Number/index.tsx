/*
 * Copyright © 2019 Cask Data, Inc.
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
import ThemeWrapper from 'components/ThemeWrapper';
import PropTypes from 'prop-types';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import TextField from '@material-ui/core/TextField';

const styles = (theme) => {
  return {
    root: {
      // border: `1px solid ${theme.palette.grey['300']}`,
      // borderRadius: 4,
      // margin: '10px 0 10px 10px',
    },
  };
};

interface INumberProps extends WithStyles<typeof styles> {
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
  isFieldRequired: boolean;
}

function Number({ value, onChange, disabled, isFieldRequired, classes }: INumberProps) {
  // not required and not disabled by default
  // default value for number input?
  const onChangeHandler = (event: React.ChangeEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (v && typeof onChange === 'function') {
      onChange(v);
    }
  };

  return (
    <TextField
      type="number"
      value={value}
      onChange={onChangeHandler}
      required={isFieldRequired}
      disabled={disabled}
      InputProps={{
        classes,
      }}
    />
  );
}

const StyledNumber = withStyles(styles)(Number);

export default function StyledNumberWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledNumber {...props} />
    </ThemeWrapper>
  );
}

(StyledNumber as any).propTypes = {
  value: PropTypes.string,
  disabled: PropTypes.bool,
  isFieldRequired: PropTypes.bool,
};
