{{/* vim: set filetype=mustache: */}}

{{/*
Kadalu KADALU_VERSION
*/}}
{{- define "common.version" -}}
{{- if .Values.global.kadaluVersion -}}
{{- .Values.global.kadaluVersion -}}
{{- else if eq .Chart.Version "0.0.0-dev.0" -}}
{{- "devel" -}}
{{- else -}}
{{- .Chart.Version -}}
{{- end -}}
{{- end -}}
