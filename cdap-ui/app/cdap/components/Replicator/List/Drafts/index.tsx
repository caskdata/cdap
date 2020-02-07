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
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyReplicatorApi } from 'api/replicator';
import { humanReadableDate, objectQuery } from 'services/helpers';
import { PluginType } from 'components/Replicator/constants';
import { Link } from 'react-router-dom';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    row: {
      color: theme.palette.grey[50],
      '&:hover': {
        color: 'inherit',
      },
    },
    headerText: {
      marginBottom: '10px',
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

const DraftsView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [drafts, setDrafts] = React.useState([]);

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
    };

    MyReplicatorApi.listDrafts(params).subscribe((list) => {
      setDrafts(list);
    });
  }, []);

  return (
    <div className={classes.root}>
      <div className={classes.headerText}>
        {drafts.length} {drafts.length === 1 ? 'draft' : 'drafts'} - Select a row to edit draft
      </div>
      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Draft name</div>
              <div>From / To</div>
              <div>Created</div>
              <div>Updated</div>
            </div>
          </div>

          <div className="grid-body">
            {drafts.map((draft) => {
              const stageMap = {};
              const stages = objectQuery(draft, 'config', 'stages') || [];

              stages.forEach((stage) => {
                stageMap[stage.plugin.type] = stage.plugin.name;
              });

              const source = stageMap[PluginType.source] || '--';
              const target = stageMap[PluginType.target] || '--';

              return (
                <Link
                  to={`/ns/${getCurrentNamespace()}/replicator/drafts/${draft.name}`}
                  className={`grid-row ${classes.row}`}
                  key={draft.name}
                >
                  <div>{draft.label}</div>
                  <div>
                    {source} / {target}
                  </div>
                  <div>{humanReadableDate(draft.createdTimeMillis, true)}</div>
                  <div>{humanReadableDate(draft.updatedTimeMillis, true)}</div>
                </Link>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

const Drafts = withStyles(styles)(DraftsView);
export default Drafts;
