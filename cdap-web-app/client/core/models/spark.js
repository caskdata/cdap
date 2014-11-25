/*
 * Spark Model
 */

define(['core/models/program'], function (Program) {

    var METRICS_PATHS = {
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.BlockManager.memory.remainingMem_MB?aggregate=true': 'blockRemainingMemory',
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.BlockManager.memory.maxMem_MB?aggregate=true': 'blockMaxMemory',
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.BlockManager.memory.memUsed_MB?aggregate=true': 'blockUsedMemory',
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.BlockManager.disk.diskSpaceUsed_MB?aggregate=true': 'blockDiskSpaceUsed',
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.DAGScheduler.job.activeJobs?aggregate=true': 'schedulerActiveJobs',
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.DAGScheduler.job.allJobs?aggregate=true': 'schedulerAllJobs',
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.DAGScheduler.stage.failedStages?aggregate=true': 'schedulerFailedStages',
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.DAGScheduler.stage.runningStages?aggregate=true': 'schedulerRunningStages',
        'system/apps/{{appId}}/spark/{{programId}}/{{programId}}.DAGScheduler.stage.waitingStages?aggregate=true': 'schedulerWaitingStages'
    };

    var METRIC_TYPES = {
        'blockRemainingMemory': 'sizeNumber',
        'blockMaxMemory': 'sizeNumber',
        'blockUsedMemory': 'sizeNumber',
        'blockDiskSpaceUsed': 'sizeNumber',
        'schedulerActiveJobs': 'number',
        'schedulerAllJobs': 'number',
        'schedulerFailedStages': 'number',
        'schedulerRunningStages': 'number',
        'schedulerWaitingStages': 'number'
    };

    var METRICS = {
        blockRemainingMemory: 0,
        blockMaxMemory: 0,
        blockUsedMemory: 0,
        blockDiskSpaceUsed: 0,
        schedulerActiveJobs: 0,
        schedulerAllJobs: 0,
        schedulerFailedStages: 0,
        schedulerRunningStages: 0,
        schedulerWaitingStages: 0
    };

    var EXPECTED_FIELDS = [
        'name',
        'description'
    ];

    var Model = Program.extend({
        type: 'Spark',
        plural: 'Spark',
        href: function () {
            return '#/spark/' + this.get('id');
        }.property('id'),

        currentState: '',

        init: function () {
            var self = this;
            this._super();
            this.set('id', this.get('app') + ':' + this.get('name'));
            this.set('description', this.get('meta') || 'Spark');
            if (this.get('meta')) {
                this.set('startTime', this.get('meta').startTime);
            }

            this.set('metricsData', Em.ArrayProxy.create({content: []}));
            //populate Ember Array for metrics
            $.each(METRICS, function (k, v) {
                var type = METRIC_TYPES[k];
                self.get('metricsData').pushObject(
                    Em.Object.create({name: k, value: v, type: type, showMB: type === 'sizeNumber'}));
            })
        },

        start: function (http) {
            var model = this;
            model.set('currentState', 'STARTING');

            http.post('rest', 'apps', this.get('app'), 'spark', model.get('name'), 'start',
                function (response) {

                    if (response.error) {
                        C.Modal.show(response.error, response.message);
                    } else {
                        model.set('lastStarted', new Date().getTime() / 1000);
                    }

                });
        },

        stop: function (http) {
            var model = this;
            model.set('currentState', 'STOPPING');

            http.post('rest', 'apps', this.get('app'), 'spark', model.get('name'), 'stop',
                function (response) {

                    if (response.error) {
                        C.Modal.show(response.error, response.message);
                    } else {
                        model.set('lastStarted', new Date().getTime() / 1000);
                    }

                });
        },

        getStartDate: function () {
            var time = parseInt(this.get('startTime'), 10);
            return new Date(time).toString('MMM d, yyyy');
        }.property('startTime'),

        getStartHours: function () {
            var time = parseInt(this.get('startTime'), 10);
            return new Date(time).toString('hh:mm tt');
        }.property('startTime'),

        context: function () {

            return this.interpolate('apps/{parent}/spark/{id}');

        }.property('app', 'name'),

        interpolate: function (path) {

            return path.replace(/\{parent\}/, this.get('app'))
                .replace(/\{id\}/, this.get('name'));

        },

        startStopDisabled: function () {

            if (this.currentState === 'STARTING' ||
                this.currentState === 'STOPPING') {
                return true;
            }

            return false;

        }.property('currentState'),

        getMetricsRequest: function (http) {
            var self = this;
            var appId = this.get('app');
            var programId = this.get('id');
            var paths = [];
            var pathMap = {};
            for (var path in METRICS_PATHS) {
                var url = new S(path).template({'appId': appId, 'programId': programId}).s;
                paths.push(url);
                pathMap[url] = METRICS_PATHS[path];
            }
            http.post('metrics', paths, function (response, status) {
                if (!response.result) {
                    return;
                }
                var result = response.result;
                var i = result.length, metric;
                while (i--) {
                    metric = pathMap[result[i]['path']];
                    if (metric) {
                        if (result[i]['result']['data'] instanceof Array) {
                            result[i]['result']['data'] = result[i]['result']['data'].map(function (entry) {
                                return entry.value;
                            });
                            self.setMetricData(metric, result[i]['result']['data']);
                        }
                        else if (metric.type === 'number' || metric.type === 'sizeNumber') {
                            self.setMetricData(metric, C.Util.numberArrayToString(result[i]['result']['data']));
                        } else {
                            self.setMetricData(metric, result[i]['result']['data']);
                        }
                    }
                    metric = null;
                }
            });
        },

        setMetricData: function (name, value) {
            var metricsData = this.get('metricsData');
            metricsData.set(name, value);
        }
    });

    Model.reopenClass({
        type: 'Spark',
        kind: 'Model',
        find: function (model_id, http) {

            model_id = model_id.split(':');

            var self = this,
                promise = Ember.Deferred.create(),
                app_id = model_id[0],
                spark_id = model_id[1];

            http.rest('apps', app_id, 'spark', spark_id, function (model, error) {

                model = self.transformModel(model);

                model.app = app_id;
                model = C.Spark.create(model);
                model.id = spark_id;
                model.name = spark_id;

                http.rest('apps', app_id, 'spark', spark_id, 'status', function (response) {
                    if (!$.isEmptyObject(response)) {
                        model.set('currentState', response.status);
                        promise.resolve(model);
                    } else {
                        promise.reject('Status not found');
                    }
                });

            });

            return promise;
        },

        transformModel: function (model) {

            var newModel = {};
            for (var i = EXPECTED_FIELDS.length - 1; i >= 0; i--) {
                newModel[EXPECTED_FIELDS[i]] = model[EXPECTED_FIELDS[i]];
            }
            if ('appId' in model || 'applicationId' in model) {
                newModel.appId = model.appId || model.applicationId;
            }
            return newModel;

        }
    });

    return Model;

});