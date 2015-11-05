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

angular.module(PKG.name + '.feature.hydrator')
  .service('NodeConfigStore', function(PipelineNodeConfigDispatcher, $q, MyAppDAGService, $filter, IMPLICIT_SCHEMA, GLOBALS, myPipelineApi, $state, $rootScope) {

    var dispatcher = PipelineNodeConfigDispatcher.getDispatcher();
    this.changeListeners = [];
    this.setDefaults = function() {
      this.state = {
        plugin: {},
        isValidPlugin: false,
        isSource: false,
        isSink: false,
        isTransform: false
      };
    };
    this.setDefaults();

    this.getState = function() {
      return this.state;
    };

    this.setState = function(plugin) {
      onPluginChange.call(this, plugin);
    };

    this.registerOnChangeListener = function(callback) {
      this.changeListeners.push(callback);
    };
    this.emitChange = function() {
      this.changeListeners.forEach(function(callback) {
        callback();
      });
    };

    dispatcher.register('onPluginChange', this.setState.bind(this));
    dispatcher.register('onPluginRemove', function() {
      this.setDefaults();
      this.emitChange();
    }.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this));

    function onPluginChange(plugin) {
      if (plugin && this.state.plugin && plugin.id === this.state.plugin.id) {
        return;
      }
      switchPlugin.call(this, plugin)
        .then(function() {
          this.emitChange();
        }.bind(this));
    }

    function switchPlugin(plugin) {
      // This is super wrong. While re-writing this in flux architecture this should go away.
      this.state.plugin = MyAppDAGService.nodes[plugin.id];
      this.state.isValidPlugin = false;
      // // falsify the ng-if in the template for one tick so that the template gets reloaded
      // // there by reloading the controller.
      // $timeout(setPluginInfo.bind(this));
      return setPluginInfo.call(this);
    }

    function setPluginInfo() {
      this.state.isSource = false;
      this.state.isTransform = false;
      this.state.isSink = false;
      return configurePluginInfo.call(this).then(
        function success() {
          this.state.isValidPlugin = Object.keys(this.state.plugin).length;
        }.bind(this),
        function error() {
          console.error('Fetching backend properties for :',this.state.plugin.name, ' failed.');
        });
    }

    function configurePluginInfo() {
      var defer = $q.defer();

      var pluginId = this.state.plugin.id;
      var input;
      var sourceConn = $filter('filter')(MyAppDAGService.connections, { target: pluginId });
      var sourceSchema = null;
      var isStreamSource = false;

      var clfSchema = IMPLICIT_SCHEMA.clf;

      var syslogSchema = IMPLICIT_SCHEMA.syslog;

      var source;
      if (sourceConn.length) {
        source = MyAppDAGService.nodes[sourceConn[0].source];
        sourceSchema = source.outputSchema;

        if (source.name === 'Stream') {
          isStreamSource = true;
        }

        if (source.properties.format && source.properties.format === 'clf') {
          sourceSchema = clfSchema;
        } else if (source.properties.format && source.properties.format === 'syslog') {
          sourceSchema = syslogSchema;
        }

      } else {
        sourceSchema = MyAppDAGService.nodes[pluginId].properties.schema || '';
      }

      fetchBackendProperties
        .call(this, this.state.plugin)
        .then(
        function success() {
          var artifactTypeExtension = GLOBALS.pluginTypes[MyAppDAGService.metadata.template.type];
          try {
            input = JSON.parse(sourceSchema);
          } catch (e) {
            input = null;
          }

          if (isStreamSource) {
            // Must be in this order!!
            if (!input) {
              input = {
                fields: [{ name: 'body', type: 'string' }]
              };
            }

            input.fields.unshift({
              name: 'headers',
              type: {
                type: 'map',
                keys: 'string',
                values: 'string'
              }
            });

            input.fields.unshift({
              name: 'ts',
              type: 'long'
            });
          }

          this.state.plugin.inputSchema = input ? input.fields : null;
          angular.forEach(this.state.plugin.inputSchema, function (field) {
            if (angular.isArray(field.type)) {
              field.type = field.type[0];
              field.nullable = true;
            } else {
              field.nullable = false;
            }
          });

          if (!this.state.plugin.outputSchema && input) {
            this.state.plugin.outputSchema = JSON.stringify(input) || null;
          }

          if (this.state.plugin.type === artifactTypeExtension.source) {
            this.state.isSource = true;
          }

          if (this.state.plugin.type === artifactTypeExtension.sink) {
            this.state.isSink = true;
          }
          if (this.state.plugin.type === 'transform') {
            this.state.isTransform = true;
          }
          defer.resolve(true);
        }.bind(this),
        function error() {
          defer.reject(false);
        }
      );

      return defer.promise;
    }

    function fetchBackendProperties(plugin) {
      var defer = $q.defer();
      var sourceType = GLOBALS.pluginTypes[MyAppDAGService.metadata.template.type].source,
          sinkType = GLOBALS.pluginTypes[MyAppDAGService.metadata.template.type].sink;

      var propertiesApiMap = {
        'transform': myPipelineApi.fetchTransformProperties
      };
      propertiesApiMap[sourceType] = myPipelineApi.fetchSourceProperties;
      propertiesApiMap[sinkType] = myPipelineApi.fetchSinkProperties;

      // This needs to pass on a scope always. Right now there is no cleanup
      // happening
      var params = {
        namespace: $state.params.namespace,
        pipelineType: MyAppDAGService.metadata.template.type,
        version: $rootScope.cdapVersion,
        extensionType: plugin.type,
        pluginName: plugin.name
      };

      return propertiesApiMap[plugin.type](params)
        .$promise
        .then(function(res) {

          var pluginProperties = (res.length? res[0].properties: {});
          if (res.length && (!plugin.description || (plugin.description && !plugin.description.length))) {
            plugin.description = res[0].description;
          }
          plugin._backendProperties = pluginProperties;
          defer.resolve(plugin);
          return defer.promise;
        });
    }

  });
