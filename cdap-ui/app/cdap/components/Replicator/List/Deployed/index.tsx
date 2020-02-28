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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { PluginType } from 'components/Replicator/constants';
import { objectQuery } from 'services/helpers';
import Status from 'components/Status';
import { Link } from 'react-router-dom';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    headerText: {
      marginBottom: '10px',
    },
    row: {
      color: theme.palette.grey[50],
      '&:hover': {
        color: 'inherit',
      },
    },
    gridWrapper: {
      // 100% - headerText
      height: 'calc(100% - 20px)',
      '& .grid.grid-container.grid-compact': {
        maxHeight: '100%',
      },
    },
  };
};

const DeployedView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [replicators, setReplicators] = React.useState([]);
  const [statusMap, setStatusMap] = React.useState({});
  const [configMap, setConfigMap] = React.useState({});

  // TODO: Replace with GraphQL
  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
    };

    MyReplicatorApi.list(params).subscribe((list) => {
      setReplicators(list);

      // Batch status
      const batchStatusBody = list.map((replicator) => {
        return {
          appId: replicator.name,
          programType: 'worker',
          programId: 'DeltaWorker',
        };
      });

      MyReplicatorApi.batchStatus(params, batchStatusBody).subscribe((status) => {
        const map = {};

        status.forEach((replicator) => {
          map[replicator.appId] = replicator.status;
        });

        setStatusMap(map);
      });

      const batchDetailBody = list.map((replicator) => {
        return {
          appId: replicator.name,
        };
      });

      MyReplicatorApi.batchAppDetail(params, batchDetailBody).subscribe((apps) => {
        const map = {};

        apps.forEach((app) => {
          if (!app.detail) {
            return;
          }
          const replicator = app.detail;

          let config;
          try {
            config = JSON.parse(replicator.configuration);
          } catch (e) {
            // tslint:disable-next-line: no-console
            console.log('Failed to parse replicator configuration', e);
            return;
          }

          const replicatorObj = {};
          config.stages.forEach((stage) => {
            replicatorObj[stage.plugin.type] = stage.plugin.name;
          });

          map[replicator.name] = replicatorObj;
        });

        setConfigMap(map);
      });
    });
  }, []);

  return (
    <div className={classes.root}>
      <div className={classes.headerText}>
        {replicators.length} Delta {replicators.length === 1 ? 'replicator' : 'replicators'} -
        Select a row to view details
      </div>

      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Replicator name</div>
              <div>From / To</div>
              <div>Status</div>
            </div>
          </div>

          <div className="grid-body">
            {replicators.map((replicator) => {
              const source = objectQuery(configMap, replicator.name, PluginType.source) || '--';
              const target = objectQuery(configMap, replicator.name, PluginType.target) || '--';

              return (
                <Link
                  to={`/ns/${getCurrentNamespace()}/replicator/detail/${replicator.name}`}
                  className={`grid-row ${classes.row}`}
                  key={replicator.name}
                >
                  <div>{replicator.name}</div>
                  <div>
                    {source} / {target}
                  </div>
                  <div>
                    <Status status={statusMap[replicator.name]} />
                  </div>
                </Link>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

const Deployed = withStyles(styles)(DeployedView);
export default Deployed;
