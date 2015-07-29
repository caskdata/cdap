angular.module(PKG.name + '.feature.adapters')
  .controller('_AdapterCreateController', function(MyPlumbService, myAdapterApi, $bootstrapModal, $scope, rConfig, $stateParams, $alert, $modalStack) {
    this.metadata = MyPlumbService.metadata;
    if (rConfig) {
      this.data =  rConfig;
    }
    if ($stateParams.name) {
      this.metadata.name = $stateParams.name;
    }
    if ($stateParams.type) {
      if (['ETLBatch', 'ETLRealtime'].indexOf($stateParams.type) !== -1) {
        this.metadata.template.type = $stateParams.type;
      } else {
        $alert({
          type: 'danger',
          content: 'Invalid template type. Has to be either ETLBatch or ETLRealtime'
        });
      }
    }

    myAdapterApi.fetchTemplates({
      scope: $scope
    })
      .$promise
      .then(function(res) {
        this.adapterTypes = res;
      }.bind(this));

    this.showMetadataModal = function() {
      if (this.metadata.error) {
        delete this.metadata.error;
      }

      $bootstrapModal.open({
        templateUrl: '/assets/features/adapters/templates/create/metadata.html',
        size: 'lg',
        windowClass: 'adapter-modal',
        keyboard: true,
        controller: ['$scope', function($scope) {
          $scope.modelCopy = angular.copy(this.metadata);
          $scope.metadata = this.metadata;
          $scope.reset = function () {
            this.metadata.name = $scope.modelCopy.name;
            this.metadata.description = $scope.modelCopy.description;
          }.bind(this);
        }.bind(this)]
      });
    };

    $scope.$on('$destroy', function() {
      $modalStack.dismissAll();
    });
  });
