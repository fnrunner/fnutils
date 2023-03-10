/*
Copyright 2023 Nokia.

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

package meta

import (
	"encoding/json"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	emptyGvk  = "empty gvk"
	emptyKind = "empty kind in gvk"
)

func GVKToString(gvk *schema.GroupVersionKind) string {
	if gvk == nil {
		return emptyGvk
	}

	if gvk.Kind == "" {
		return emptyKind
	}
	var sb strings.Builder
	sb.WriteString(gvk.Kind)
	if gvk.Version != "" {
		sb.WriteString("." + gvk.Version)
	}
	if gvk.Group != "" {
		sb.WriteString("." + gvk.Group)
	}
	return sb.String()

}

func StringToGroupVersionKind(s string) (string, string, string) {
	if strings.Count(s, ".") >= 2 {
		s := strings.SplitN(s, ".", 3)
		return s[2], s[1], s[0]
	}
	return "", "", ""
}

func StringToGVK(s string) *schema.GroupVersionKind {
	group, version, kind := StringToGroupVersionKind(s)
	return &schema.GroupVersionKind{
		Group:   group,
		Version: version,
		Kind:    kind,
	}
}

func apiVersionToGroupVersion(apiVersion string) (string, string) {
	split := strings.Split(apiVersion, "/")
	if len(split) > 1 {
		return split[0], strings.Join(split[1:], "/")
	}
	return "", apiVersion
}

func GetGVKFromAPIVersionKind(apiVersion, kind string) *schema.GroupVersionKind {
	ownerGroup, ownerVersion := apiVersionToGroupVersion(apiVersion)

	return &schema.GroupVersionKind{
		Group: ownerGroup, Version: ownerVersion, Kind: kind}
}

func GetGVKFromObject(o client.Object) *schema.GroupVersionKind {
	return &schema.GroupVersionKind{
		Group:   o.GetObjectKind().GroupVersionKind().Group,
		Version: o.GetObjectKind().GroupVersionKind().Version,
		Kind:    o.GetObjectKind().GroupVersionKind().Kind,
	}
}

func GetGVKFromRuntimeRawExtension(gvr runtime.RawExtension) (*schema.GroupVersionKind, error) {
	var u unstructured.Unstructured
	if err := json.Unmarshal(gvr.Raw, &u); err != nil {
		return nil, err
	}
	gv, err := schema.ParseGroupVersion(u.GetAPIVersion())
	if err != nil {
		return nil, err
	}

	return &schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    u.GetKind(),
	}, nil
}
