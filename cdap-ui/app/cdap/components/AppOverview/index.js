/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {PropTypes, Component} from 'react';

export default class AppOverview extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleOverview: this.props.toggleOverview,
      entity: this.props.entity
    };
  }
  componentWillReceiveProps(nextProps) {
    let {toggleOverview, entity } = nextProps;
    if (
      this.props.toggleOverview !== toggleOverview ||
      this.props.entity !== entity
    ) {
      this.setState({
        toggleOverview,
        entity
      });
    }
  }
  hideOverview() {
    this.setState({
      toggleOverview: false,
      entity: null
    });
    if (this.props.onClose) {
      this.props.onClose();
    }
  }
  render() {
    return (
      <div className="overview-container">
        <div className="overview-wrapper" onClick={this.hideOverview.bind(this, null)}>
          <pre>
            {JSON.stringify(this.state.entity, null, 2)}
          </pre>
        </div>
      </div>
    );
  }
}

AppOverview.propTypes = {
  toggleOverview: PropTypes.bool,
  entity: PropTypes.object,
  onClose: PropTypes.func
};
