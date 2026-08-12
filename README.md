# kaDalu

[![Operator Docker Pulls](https://img.shields.io/docker/pulls/kadalu/kadalu-operator.svg?label=DockerPulls%20Operator)](https://img.shields.io/docker/pulls/kadalu/kadalu-operator.svg)
[![Server Docker Pulls](https://img.shields.io/docker/pulls/kadalu/kadalu-server.svg?label=DockerPulls%20Server)](https://img.shields.io/docker/pulls/kadalu/kadalu-server.svg)
![Devel](https://github.com/joejulian/kadalu/actions/workflows/on-pr-merge.yml/badge.svg)
![Release](https://github.com/joejulian/kadalu/actions/workflows/on-release-tag.yml/badge.svg)

## What is Kadalu ?

[Kadalu](https://kadalu.io) is a project to provide Persistent Storage in container ecosystem (like kubernetes, openshift, RKE, etc etc). Kadalu operator deploys CSI pods, and **gluster storage** pods as per the config. You would get your PVs served through APIs implemented in CSI.

## Kubernetes compatibility

This fork supports [Kubernetes 1.36](https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/)
and uses Kubernetes 1.36 as its current compatibility-validation target. Older
Kubernetes releases may still work, but they are not covered by the current
validation gate.

Source development and test tooling use Python 3.12 as the baseline. Runtime
and development dependencies are installed from the locked requirement files
under [`requirements/`](requirements/).

## Get Started

Getting started is made easy to copy paste the below commands.

```console
curl -fsSL https://github.com/joejulian/kadalu/releases/latest/download/install.sh | sudo bash -x
kubectl-kadalu version
kubectl kadalu install --type=$K8S_DIST
```

Where `K8S_DIST` can be one of below values and `kubernetes` being the default:
- kubernetes
- openshift
- rke
- microk8s

The above will deploy the latest version of kadalu operator and CSI pods. Once done, you can provide storage to kadalu operator to manage.

```
$ kubectl kadalu storage-add storage-pool-1 --device kube1:/dev/sdc
```

Note that, in above command, `kube1` is the node which is providing `/dev/sdc` as a storage to kadalu. In your setup, this may be different.

If you made some errors in setup and want to start fresh, check the
[cleanup script](extras/scripts/cleanup). It removes only Kadalu-labelled or
explicitly named resources and preserves the namespace by default. Set
`KADALU_DELETE_NAMESPACE=true` only when the namespace is dedicated to Kadalu
and should also be removed.

```
curl -s https://raw.githubusercontent.com/joejulian/kadalu/devel/extras/scripts/cleanup | bash
```


## Reach out

1. Best is opening an [issue in github.](https://github.com/kadalu/kadalu/issues)
2. Reach to us on [Slack](https://join.slack.com/t/kadalu/shared_invite/enQtNzg1ODQ0MDA5NTM2LWMzMTc5ZTJmMjk4MzI0YWVhOGFlZTJjZjY5MDNkZWI0Y2VjMDBlNzVkZmI1NWViN2U3MDNlNDJhNjE5OTBlOGU) (Note, there would be no history) - https://kadalu.slack.com


## Contributing

We would like your contributions to come as feedbacks, testing, development etc. See [CONTRIBUTING](CONTRIBUTING.md) for more details.

If you are interested in financial donation to the project, or to the developers, you can do so at our [opencollective](https://opencollective.com/kadalu) page. (We like github sponsors too, but its still in waiting list for an org in India).


## Helm support

`helm install kadalu --namespace kadalu --create-namespace https://github.com/joejulian/kadalu/releases/latest/download/kadalu-helm-chart.tgz --set-string global.kubernetesDistro=$K8S_DIST --set operator.enabled=true`

Where `K8S_DIST` can be one of below values:
- kubernetes
- openshift
- rke
- microk8s

If `global.kubernetesDistro` is not supplied, `kubernetes` is used by default.

NOTE: We are still evolving with Helm chart based development, and happy to get contributions on the same.

## Platform supports

The fork publishes and verifies multi-architecture images for x86_64 (amd64) and 64-bit ARM (arm64), including 64-bit Raspberry Pi operating systems. The current release pipeline does not publish 32-bit arm/v7 images.

For any other platforms, build the images locally with `make build-containers` after checking out the repository. Publishing releases is intentionally handled only by the tag-driven GitHub Actions workflow.


## How to pronounce kadalu ?

One is free to pronounce 'kaDalu' as they wish. Below is a sample of how we pronounce it!

[<img src="https://raw.githubusercontent.com/kadalu/kadalu/devel/extras/assets/speaker.svg" width="64"/>](https://raw.githubusercontent.com/kadalu/kadalu/devel/extras/assets/kadalu_01.wav)


>
>**Request:** If you like the project, give a github star :-)
>
