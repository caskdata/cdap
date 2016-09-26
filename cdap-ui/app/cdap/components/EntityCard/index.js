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

import React, {Component, PropTypes} from 'react';
import Card from '../Card';
import EntityCardHeader from './EntityCardHeader';
import ApplicationMetrics from './ApplicationMetrics';
import ArtifactMetrics from './ArtifactMetrics';
import DatasetMetrics from './DatasetMetrics';
import StreamMetrics from './StreamMetrics';
import classnames from 'classnames';
import FastActions from 'components/EntityCard/FastActions';

require('./EntityCard.less');

export default class EntityCard extends Component {
  constructor(props) {
    super(props);
  }

  renderEntityStatus() {
    switch (this.props.entity.type) {
      case 'application':
        return <ApplicationMetrics entity={this.props.entity} />;
      case 'artifact':
        return <ArtifactMetrics entity={this.props.entity} />;
      case 'datasetinstance':
        return <DatasetMetrics entity={this.props.entity} />;
      case 'stream':
        return <StreamMetrics entity={this.props.entity} />;
      case 'view':
        return null;
    }
  }

  render() {
    const header = (
      <EntityCardHeader
        type={this.props.entity.type}
        systemTags={this.props.entity.metadata.metadata.SYSTEM.tags}
      />
    );

    return (
      <Card
        header={header}
        cardClass={`home-cards ${this.props.entity.type}`}
      >
        <div className="entity-id-container">
          <h4
            className={classnames({'with-version': this.props.entity.version})}
          >
            {this.props.entity.id}
          </h4>
          <small>{this.props.entity.version}</small>
        </div>

        {this.renderEntityStatus()}

        <div className="fast-actions-container">
          <FastActions
            entity={this.props.entity}
            onUpdate={this.props.onUpdate}
          />
        </div>
      </Card>
    );
  }
}

EntityCard.propTypes = {
  entity: PropTypes.object,
  onUpdate: PropTypes.func
};
