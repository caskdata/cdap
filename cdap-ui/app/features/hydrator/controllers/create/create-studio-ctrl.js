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

class HydratorCreateStudioController {
  constructor(LeftPanelStore, LeftPanelActionsFactory, ConfigActionsFactory, $stateParams, rConfig, ConfigStore, $rootScope) {
    // This is required because before we fireup the actions related to the store, the store has to be initialized to register for any events.
    ConfigStore.setDefaults();
    if ($stateParams.type) {
      ConfigActionsFactory.setArtifact({
        version: $rootScope.cdapVersion,
        name: $stateParams.type,
        scope: 'SYSTEM'
      });
    }
    if (rConfig) {
      ConfigActionsFactory.setArtifact(rConfig.artifact);
      ConfigActionsFactory.setName(rConfig.name);
      ConfigActionsFactory.setDescription(rConfig.description);
    }
  }
}

HydratorCreateStudioController.$inject = ['LeftPanelStore', 'LeftPanelActionsFactory', 'ConfigActionsFactory', '$stateParams', 'rConfig', 'ConfigStore', '$rootScope'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorCreateStudioController', HydratorCreateStudioController);

    // this.toggleSidebar = function(isExpanded) {
    //   if (isExpanded) {
    //     LeftPanelActionsFactory.minize();
    //   } else {
    //     LeftPanelActionsFactory.expand();
    //   }
    // };

    //
    // var confirmOnPageExit = function (e) {
    //
    //   if (!MyAppDAGService.isConfigTouched) { return; }
    //   // If we haven't been passed the event get the window.event
    //   e = e || $window.event;
    //   var message = 'You have unsaved changes.';
    //   // For IE6-8 and Firefox prior to version 4
    //   if (e) {
    //     e.returnValue = message;
    //   }
    //   // For Chrome, Safari, IE8+ and Opera 12+
    //   return message;
    // };
    // $window.onbeforeunload = confirmOnPageExit;
    //
    // $scope.$on('$stateChangeStart', function (event) {
    //   if (MyAppDAGService.isConfigTouched) {
    //     var response = confirm('You have unsaved changes. Are you sure you want to exit this page?');
    //     if (!response) {
    //       event.preventDefault();
    //     }
    //   }
    // });
    //
    // if (rConfig) {
    //   $timeout(function() {
    //     MyAppDAGService.setNodesAndConnectionsFromDraft(rConfig);
    //   });
    // }
    //
    // $scope.$on('$destroy', function() {
    //   $modalStack.dismissAll();
    //   MyConsoleTabService.resetMessages();
    //   $window.onbeforeunload = null;
    //   EventPipe.cancelEvent('plugin.reset');
    //   EventPipe.cancelEvent('schema.clear');
    // });
    //
    // this.toggleSidebar = function() {
    //   this.isExpanded = !this.isExpanded;
    // };
  // });
