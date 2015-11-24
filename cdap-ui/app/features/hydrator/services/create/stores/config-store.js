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

class ConfigStore {
  constructor(ConfigDispatcher, CanvasFactory, GLOBALS){
    this.state = {};
    this.CanvasFactory = CanvasFactory;
    this.GLOBALS = GLOBALS;
    this.changeListeners = [];
    this.setDefaults();
    this.configDispatcher = ConfigDispatcher.getDispatcher();
    this.configDispatcher.register('onArtifactSave', this.setArtifact.bind(this));
    this.configDispatcher.register('onMetadataInfoSave', this.setMetadataInformation.bind(this));
    this.configDispatcher.register('onPluginAdd', this.setConfig.bind(this));
  }
  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }
  setDefaults() {
    this.state = {
      artifact: {
        name: '',
        scope: 'SYSTEM',
        version: ''
      },
      config: {
        source: {},
        sinks: [],
        transforms: []
      },
      connections: [],
      __ui__: {
        nodes: [],
      },
      description: '',
      name: ''
    };
  }
  getDefaultConfig() {
    return {
      source: {},
      sinks: [],
      transforms: []
    };
  }

  setState(state) {
    this.state = state;
  }
  getState() {
    return this.state;
  }
  getArtifact() {
    return this.state.artifact;
  }
  getAppType() {
    return this.state.artifact.name;
  }
  getConnections() {
    return this.state.connections;
  }
  getConfig() {
    return this.state.config;
  }
  generateConfigFromState() {
    var config = this.getDefaultConfig();
    var artifactTypeExtension = this.GLOBALS.pluginTypes[this.state.artifact.name];
    var nodesMap = {};
    this.state.__ui__.nodes.forEach(function(n) {
      nodesMap[n.id] = n;
    });

    function addPluginToConfig(plugin, id) {
      var pluginConfig =  {
        // Solely adding id and _backendProperties for validation.
        // Should be removed while saving it to backend.
        id: plugin.id,
        name: plugin.name,
        label: plugin.label,
        properties: plugin.properties,
        _backendProperties: plugin._backendProperties,
        outputSchema: plugin.outputSchema
      };

      if (plugin.type === artifactTypeExtension.source) {
        config['source'] = pluginConfig;
      } else if (plugin.type === 'transform') {
        if (plugin.errorDatasetName && plugin.errorDatasetName.length > 0) {
          pluginConfig.errorDatasetName = plugin.errorDatasetName;
        }
        if (plugin.validationFields) {
          pluginConfig.validationFields = plugin.validationFields;
        }

        config['transforms'].push(pluginConfig);
      } else if (plugin.type === artifactTypeExtension.sink) {
        config['sinks'].push(pluginConfig);
      }
      delete nodesMap[id];
    }
    var connections = this.CanvasFactory.orderConnections(
      angular.copy(this.state.connections),
      this.state.artifact.name,
      this.state.__ui__.nodes
    );

    connections.forEach( connection => {
      if (nodesMap[connection.source]) {
        addPluginToConfig(nodesMap[connection.source], connection.source);
      }
      if (nodesMap[connection.target]) {
        addPluginToConfig(nodesMap[connection.target], connection.target);
      }
     });
    return config;
  }
  getConfigForExport() {
    var config = this.generateConfigFromState();
    this.CanvasFactory.pruneProperties(config);
    this.state.config = angular.copy(config);
    return angular.copy(this.state);
  }
  getDisplayConfig() {
    var stateCopy = this.getConfigForExport();
    delete stateCopy.__ui__;
    return stateCopy;
  }
  getDescription() {
    return this.state.description;
  }
  getName() {
    return this.state.name;
  }
  setName(name) {
    this.state.name = name;
    this.emitChange();
  }
  setDescription(description) {
    this.state.description = description;
    this.emitChange();
  }
  setMetadataInformation(name, description) {
    this.state.name = name;
    this.state.description = description;
    this.emitChange();
  }
  setConfig(config, type) {
    switch(type) {
      case 'source':
        this.state.config.source = config;
        break;
      case 'sink':
        this.state.config.sinks.push(config);
        break;
      case 'transform':
        this.state.config.transforms.push(config);
        break;
    }
    this.emitChange();
  }
  setArtifact(artifact) {
    this.state.artifact.name = artifact.name;
    this.state.artifact.version = artifact.version;
    this.state.artifact.scope = artifact.scope;
    this.emitChange();
  }
  setNodes(nodes) {
    this.state.__ui__.nodes = nodes;
  }
  setConnections(connections) {
    this.state.connections = connections;
  }
  addNode(node) {
    this.state.__ui__.nodes.push(node);
  }
  getNodes() {
    return this.state.__ui__.nodes;
  }
  getNode(nodeId) {
    var nodes = this.state.__ui__.nodes;
    var match = nodes.filter( node => node.id === nodeId);
    if (match.length) {
      match = match[0];
    } else {
      match = null;
    }
    return match;
  }
}

ConfigStore.$inject = ['ConfigDispatcher', 'CanvasFactory', 'GLOBALS'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigStore', ConfigStore);
