/*
 * Copyright © 2015 Cask Data, Inc.
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

class HydratorCreateCanvasController {
  constructor(BottomPanelStore, NodesStore, NodesActionsFactory, ConfigStore, PipelineNodeConfigActionFactory, HydratorService, $scope, $window) {
    this.NodesStore = NodesStore;
    this.ConfigStore = ConfigStore;
    this.PipelineNodeConfigActionFactory = PipelineNodeConfigActionFactory;
    this.NodesActionsFactory = NodesActionsFactory;
    this.HydratorService = HydratorService;
    this.$scope = $scope;
    this.$window = $window;

    this.setState = () => {
      this.state = {
        setScroll: (BottomPanelStore.getPanelState() === 0? false: true)
      };
    };
    this.setState();
    BottomPanelStore.registerOnChangeListener(this.setState.bind(this));

    this.nodes = [];
    this.connections = [];

    this.updateNodesAndConnections();
    NodesStore.registerOnChangeListener(this.updateNodesAndConnections.bind(this));


    this.$scope.$on('$stateChangeStart', (event) => {
      // event.preventDefault();

      let currentConfig = this.ConfigStore.getState();
      let draft = this.ConfigStore.getDraftState();

      if (!angular.equals(currentConfig, draft)) {
        var response = confirm('Are you sure you want to navigate away?');
        if (!response) {
          event.preventDefault();
        }
      }

    });

    $window.onbeforeunload = (event) => {
      event = event || $window.event;

      let currentConfig = this.ConfigStore.getState();
      let draft = this.ConfigStore.getDraftState();

      if (!angular.equals(currentConfig, draft)) {
        let message = 'You have unsaved changes.';

        if (event) {
          event.returnValue = message;
        }
        return message;
      } else {
        return;
      }
    };
  }

  setStateAndUpdateConfigStore() {
    this.nodes = this.NodesStore.getNodes();
    this.connections = this.NodesStore.getConnections();
    this.ConfigStore.setNodes(this.nodes);
    this.ConfigStore.setConnections(this.connections);
    this.ConfigStore.setComments(this.NodesStore.getComments());
  }

  updateNodesAndConnections() {
    var activeNode = this.NodesStore.getActiveNodeId();
    if (!activeNode) {
      this.deleteNode();
    } else {
      this.setActiveNode();
    }
  }

  setActiveNode() {
    var nodeId = this.NodesStore.getActiveNodeId();
    if (!nodeId) {
      return;
    }
    var pluginNode;
    var nodeFromNodesStore;
    var nodeFromConfigStore = this.ConfigStore.getNodes().filter( node => node.name === nodeId );
    if (nodeFromConfigStore.length) {
      pluginNode = nodeFromConfigStore[0];
    } else {
      nodeFromNodesStore = this.NodesStore.getNodes().filter(node => node.name === nodeId);
      pluginNode = nodeFromNodesStore[0];
    }
    this.PipelineNodeConfigActionFactory.choosePlugin(pluginNode);
  }

  deleteNode() {
    this.setStateAndUpdateConfigStore();
    this.PipelineNodeConfigActionFactory.removePlugin();
  }

  generateSchemaOnEdge(sourceId) {
    return this.HydratorService.generateSchemaOnEdge(sourceId);
  }
}


HydratorCreateCanvasController.$inject = ['BottomPanelStore', 'NodesStore', 'NodesActionsFactory', 'ConfigStore', 'PipelineNodeConfigActionFactory', 'HydratorService', '$scope', '$window'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorCreateCanvasController', HydratorCreateCanvasController);
