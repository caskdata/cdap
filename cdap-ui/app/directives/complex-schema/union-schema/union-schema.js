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

function UnionSchemaController (avsc, SCHEMA_TYPES, SchemaHelper, $scope, $timeout) {
  'ngInject';

  var vm = this;
  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;
  let timeout;

  vm.types = [];

  vm.addType = (index) => {
    let placement = index === undefined ? 0 : index + 1;
    vm.types.splice(placement, 0, {
      type: 'string',
      displayType: 'string',
      nullable: false
    });
    vm.formatOutput();
  };

  vm.removeType = (index) => {
    vm.types.splice(index, 1);
    if (vm.types.length === 0) {
      vm.addType();
    }
    vm.formatOutput();
  };

  function init(strJson) {
    if (!strJson || strJson === 'union') {
      vm.addType();
      vm.formatOutput();
      return;
    }

    let parsed = avsc.parse(strJson, { wrapUnions: true });

    vm.types = parsed.getTypes().map(SchemaHelper.parseType);

    vm.formatOutput();
  }

  vm.formatOutput = () => {
    let outputArr = vm.types.map((item) => {
      return item.nullable ? [item.type, 'null'] : item.type;
    });

    vm.model = outputArr;

    if (typeof vm.parentFormatOutput === 'function') {
      timeout = $timeout(vm.parentFormatOutput);
    }
  };

  init(vm.model);

  $scope.$on('$destroy', () => {
    $timeout.cancel(timeout);
  });
}

angular.module(PKG.name+'.commons')
.directive('myUnionSchema', function () {
  return {
    restrict: 'E',
    templateUrl: 'complex-schema/union-schema/union-schema.html',
    controller: UnionSchemaController,
    controllerAs: 'UnionSchema',
    bindToController: true,
    scope: {
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    }
  };
})
.directive('myUnionSchemaWrapper', function ($compile) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    },
    link: (scope, element) => {
      $compile('<my-union-schema ng-model="model" parent-format-output="parentFormatOutput()" is-disabled="isDisabled"></my-union-schema')(scope, (cloned) => {
        element.append(cloned);
      });
    }
  };
});
