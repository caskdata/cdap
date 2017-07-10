/*
 * Copyright © 2017 Cask Data, Inc.
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

import isNil from 'lodash/isNil';
import cloneDeep from 'lodash/cloneDeep';
import moment from 'moment';
import numeral from 'numeral';

export const ONE_MIN_SECONDS = 60;
export const ONE_HOUR_SECONDS = ONE_MIN_SECONDS * 60;
export const ONE_DAY_SECONDS = ONE_HOUR_SECONDS * 24;
const DEFAULT_GRAPH_HEIGHT = 300;
const DEFAULT_TICKS_TOTAL = 10;
const WEEKS_TICKS_TOTAL = 7;
const SECONDS_RESOLUTION = 'sec';
const MINS_RESOLUTION = 'minutes';
const HOURS_RESOLUTION = 'hours';
const DAYS_RESOLUTION = 'days';


export function getTicksTotal({start, end}) {
  if (isNil(start) || isNil(end)) {
    return DEFAULT_TICKS_TOTAL;
  }
  let timeWindow = end - start;
  if (timeWindow < ONE_DAY_SECONDS || timeWindow === ONE_DAY_SECONDS * 7) {
    return WEEKS_TICKS_TOTAL;
  }
  return DEFAULT_TICKS_TOTAL;
}

export function getXDomain({xDomainType, runsLimit, totalRunsCount, start, end}) {
  let startDomain, endDomain;
  if (xDomainType === 'limit') {
    startDomain = totalRunsCount > runsLimit ? (totalRunsCount - runsLimit) + 1 : 1;
    endDomain = totalRunsCount > runsLimit ? totalRunsCount : runsLimit;
  }
  if (xDomainType === 'time') {
    startDomain = start;
    endDomain = end;
  }
  return [startDomain, endDomain];
}

export function getYDomain({data = {}}) {
  let maxYDomain = {y: 1}, minYDomain = {y: 0};
  if (data.length > 1) {
    maxYDomain = cloneDeep(data.reduce((prev, curr) => {
      return (prev.y > curr.y) ? prev : curr;
    }));
    minYDomain = cloneDeep(data.reduce((prev, curr) => {
      return (prev.y < curr.y) ? prev : curr;
    }));
    if (maxYDomain.y === minYDomain.y) {
      minYDomain.y = 0;
    }
  }
  if (data.length == 1) {
    maxYDomain = data[0];
  }
  return {minYDomain, maxYDomain};
}

export function getYAxisProps({domain = []}) {
  let props = {
    tickTotals: 10,
    yDomain: domain,
    tickFormat: function(d) {
      if (d < 999) {
        return d;
      }
      return numeral(d).format('0.0a');
    }
  };
  if (domain[1] === 0) {
    props.tickValues = [0, 1];
    props.tickTotals = 2;
  }
  return props;
}

export function xTickFormat({xDomainType, start, end}) {
  let lastDisplayedDate;
  return (v) => {
    if (xDomainType === 'time') {
      let timeWindow = end - start;
      let date = moment(v * 1000).format('M/D/YY');
      if (timeWindow < ONE_DAY_SECONDS) {
        date = v % 2 === 0 ? moment(v * 1000).format('HH:mm a') : null;
      }
      if (timeWindow === ONE_DAY_SECONDS) {
        date = v % 2 === 0 ? moment(v * 1000).format('HH:mm a') : null;
      }
      if (timeWindow === ONE_DAY_SECONDS * 7) {
        date = v % 2 === 0 ? moment(v * 1000).format('Do MMM') : null;
      }
      if (timeWindow >= ONE_DAY_SECONDS * 30) {
        date = v % 2 === 0 ? moment(v * 1000).format('M/D/YY') : null;
      }
      if (!isNil(date) && lastDisplayedDate !== date) {
        lastDisplayedDate = date;
        return date;
      }
      return;
    }
    return v;
  };
}

export function getGraphHeight(containerRef) {
  if (containerRef) {
    let clientRect = containerRef.getBoundingClientRect();
    return clientRect.height - 100;
  }
  return DEFAULT_GRAPH_HEIGHT;
}

export function getTimeResolution(maxYDomain) {
  let yAxisResolution = SECONDS_RESOLUTION;
  if (maxYDomain > ONE_MIN_SECONDS) {
    yAxisResolution = MINS_RESOLUTION;
  }
  if (maxYDomain > ONE_HOUR_SECONDS) {
    yAxisResolution = HOURS_RESOLUTION;
  }
  if (maxYDomain > ONE_DAY_SECONDS) {
    yAxisResolution = DAYS_RESOLUTION;
  }
  return yAxisResolution;
}

export function tickFormatBasedOnTimeResolution(timeResolution) {
  return (v) => {
    if (timeResolution === MINS_RESOLUTION) {
      return (v / ONE_MIN_SECONDS).toFixed(2);
    }
    if (timeResolution === HOURS_RESOLUTION) {
      return (v / (ONE_HOUR_SECONDS)).toFixed(2);
    }
    if (timeResolution === DAYS_RESOLUTION) {
      return (v / ONE_DAY_SECONDS).toFixed(2);
    }
    return v;
  };
}

export function getDuration(time) {
  if (typeof time !== 'number') {
    return '-';
  }
  if (time < ONE_MIN_SECONDS) {
    return `${time} seconds`;
  }
  return moment.duration(time, 'seconds').humanize();
}
