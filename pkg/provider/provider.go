/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package provider

import (
	"fmt"
	"strconv"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/golang/glog"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/metrics/pkg/apis/custom_metrics"

	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
)

type E2EProvider struct {
	client dynamic.ClientPool
	mapper apimeta.RESTMapper

	values map[provider.MetricInfo]int64
}

func NewE2EProvider(client dynamic.ClientPool, mapper apimeta.RESTMapper) provider.CustomMetricsProvider {
	return &E2EProvider{
		client: client,
		mapper: mapper,
		values: make(map[provider.MetricInfo]int64),
	}
}

func (p *E2EProvider) WebService() *restful.WebService {
	ws := new(restful.WebService)

	ws.Path("/metrics")

	ws.Route(ws.PUT("/{namespace}/{resourceType}/{name}/{metric}/{value}").To(p.updateResource).
		Param(ws.PathParameter("value", "value to set metric").DataType("integer").DefaultValue("0")))
	return ws
}

func (p *E2EProvider) updateResource(request *restful.Request, response *restful.Response) {
	namespace := request.PathParameter("namespace")
	namespaced := false
	if len(namespace) > 0 {
		namespaced = true
	}
	resourceType := request.PathParameter("resourceType")
	//name := request.PathParameter("name")
	metricName := request.PathParameter("metric")
	value := request.PathParameter("value")

	groupResource := schema.GroupResource{
		Group:    "",
		Resource: resourceType,
	}

	info := provider.MetricInfo{
		GroupResource: groupResource,
		Metric:        metricName,
		Namespaced:    namespaced,
	}

	info, _, err := info.Normalized(p.mapper)
	if err != nil {
		glog.Errorf("Error normalizing info: %s", err)
	}

	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		glog.Errorf("Error converting value to int: %s", err)
	}
	p.values[info] = i
}

func (p *E2EProvider) valueFor(groupResource schema.GroupResource, metricName string, namespaced bool) (int64, error) {
	info := provider.MetricInfo{
		GroupResource: groupResource,
		Metric:        metricName,
		Namespaced:    namespaced,
	}

	value := p.values[info]

	return value, nil
}

func (p *E2EProvider) metricFor(value int64, groupResource schema.GroupResource, namespace string, name string, metricName string) (*custom_metrics.MetricValue, error) {
	kind, err := p.mapper.KindFor(groupResource.WithVersion(""))
	if err != nil {
		return nil, err
	}

	return &custom_metrics.MetricValue{
		DescribedObject: custom_metrics.ObjectReference{
			APIVersion: groupResource.Group + "/" + runtime.APIVersionInternal,
			Kind:       kind.Kind,
			Name:       name,
			Namespace:  namespace,
		},
		MetricName: metricName,
		Timestamp:  metav1.Time{time.Now()},
		Value:      *resource.NewMilliQuantity(value*100, resource.DecimalSI),
	}, nil
}

func (p *E2EProvider) metricsFor(totalValue int64, groupResource schema.GroupResource, metricName string, list runtime.Object) (*custom_metrics.MetricValueList, error) {
	if !apimeta.IsListType(list) {
		return nil, fmt.Errorf("returned object was not a list")
	}

	res := make([]custom_metrics.MetricValue, 0)

	err := apimeta.EachListItem(list, func(item runtime.Object) error {
		objMeta := item.(metav1.Object)
		value, err := p.metricFor(0, groupResource, objMeta.GetNamespace(), objMeta.GetName(), metricName)
		if err != nil {
			return err
		}
		res = append(res, *value)

		return nil
	})
	if err != nil {
		return nil, err
	}

	for i := range res {
		res[i].Value = *resource.NewMilliQuantity(100*totalValue/int64(len(res)), resource.DecimalSI)
	}

	//return p.metricFor(value, groupResource, "", name, metricName)
	return &custom_metrics.MetricValueList{
		Items: res,
	}, nil
}

func (p *E2EProvider) GetRootScopedMetricByName(groupResource schema.GroupResource, name string, metricName string) (*custom_metrics.MetricValue, error) {
	value, err := p.valueFor(groupResource, metricName, false)
	if err != nil {
		return nil, err
	}
	return p.metricFor(value, groupResource, "", name, metricName)
}

func (p *E2EProvider) GetRootScopedMetricBySelector(groupResource schema.GroupResource, selector labels.Selector, metricName string) (*custom_metrics.MetricValueList, error) {
	// construct a client to list the names of objects matching the label selector
	client, err := p.client.ClientForGroupVersionResource(groupResource.WithVersion(""))
	if err != nil {
		glog.Errorf("unable to construct dynamic client to list matching resource names: %v", err)
		// don't leak implementation details to the user
		return nil, apierr.NewInternalError(fmt.Errorf("unable to list matching resources"))
	}

	totalValue, err := p.valueFor(groupResource, metricName, false)
	if err != nil {
		return nil, err
	}

	// we can construct a this APIResource ourself, since the dynamic client only uses Name and Namespaced
	apiRes := &metav1.APIResource{
		Name:       groupResource.Resource,
		Namespaced: false,
	}

	matchingObjectsRaw, err := client.Resource(apiRes, "").
		List(metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	return p.metricsFor(totalValue, groupResource, metricName, matchingObjectsRaw)
}

func (p *E2EProvider) GetNamespacedMetricByName(groupResource schema.GroupResource, namespace string, name string, metricName string) (*custom_metrics.MetricValue, error) {
	value, err := p.valueFor(groupResource, metricName, true)
	if err != nil {
		return nil, err
	}
	return p.metricFor(value, groupResource, namespace, name, metricName)
}

func (p *E2EProvider) GetNamespacedMetricBySelector(groupResource schema.GroupResource, namespace string, selector labels.Selector, metricName string) (*custom_metrics.MetricValueList, error) {
	// construct a client to list the names of objects matching the label selector
	client, err := p.client.ClientForGroupVersionResource(groupResource.WithVersion(""))
	if err != nil {
		glog.Errorf("unable to construct dynamic client to list matching resource names: %v", err)
		// don't leak implementation details to the user
		return nil, apierr.NewInternalError(fmt.Errorf("unable to list matching resources"))
	}

	totalValue, err := p.valueFor(groupResource, metricName, true)
	if err != nil {
		return nil, err
	}

	// we can construct a this APIResource ourself, since the dynamic client only uses Name and Namespaced
	apiRes := &metav1.APIResource{
		Name:       groupResource.Resource,
		Namespaced: true,
	}

	matchingObjectsRaw, err := client.Resource(apiRes, namespace).
		List(metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	return p.metricsFor(totalValue, groupResource, metricName, matchingObjectsRaw)
}

func (p *E2EProvider) ListAllMetrics() []provider.MetricInfo {
	// TODO: maybe dynamically generate this?
	return []provider.MetricInfo{
		{
			GroupResource: schema.GroupResource{Group: "", Resource: "pods"},
			Metric:        "packets-per-second",
			Namespaced:    true,
		},
		{
			GroupResource: schema.GroupResource{Group: "", Resource: "services"},
			Metric:        "connections-per-second",
			Namespaced:    true,
		},
		{
			GroupResource: schema.GroupResource{Group: "", Resource: "namespaces"},
			Metric:        "queue-length",
			Namespaced:    false,
		},
	}
}
