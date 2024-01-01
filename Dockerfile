# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################################################################################
##                               BUILD ARGS                                   ##
################################################################################
# This build arg allows the specification of a custom Golang image.
ARG GOLANG_IMAGE=quay.io/cuongdm8499/golang:1.21.5

# The distroless image on which the CPI manager image is built.
#
# Please do not use "latest". Explicit tags should be used to provide
# deterministic builds. Follow what kubernetes uses to build
# kube-controller-manager, for example for 1.27.x:
# https://github.com/kubernetes/kubernetes/blob/release-1.27/build/common.sh#L99
ARG DISTROLESS_IMAGE=quay.io/cuongdm8499/go-runner:v2.3.1-go1.21.5-bookworm.0

# We use Alpine as the source for default CA certificates and some output
# images
ARG ALPINE_IMAGE=quay.io/cuongdm8499/alpine:3.17.5

# cinder-csi-plugin uses Debian as a base image
ARG DEBIAN_IMAGE=quay.io/cuongdm8499/debian-base:bullseye-v1.4.3

################################################################################
##                              BUILD STAGE                                   ##
################################################################################

# Build an image containing a common ca-certificates used by all target images
# regardless of how they are built. We arbitrarily take ca-certificates from
# the amd64 Alpine image.
FROM --platform=linux/amd64 ${ALPINE_IMAGE} as certs
RUN apk add --no-cache ca-certificates


# Build all command targets. We build all command targets in a single build
# stage for efficiency. Target images copy their binary from this image.
# We use go's native cross compilation for multi-arch in this stage, so the
# builder itself is always amd64
FROM --platform=linux/amd64 ${GOLANG_IMAGE} as builder

ARG GOPROXY=https://goproxy.io,direct
ARG TARGETOS
ARG TARGETARCH
ARG VERSION

WORKDIR /build
COPY Makefile go.mod go.sum ./
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN make build GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOPROXY=${GOPROXY} VERSION=${VERSION}


################################################################################
##                             TARGET IMAGES                                  ##
################################################################################

##
## vcontainer-cloud-controller-manager
##
FROM --platform=${TARGETPLATFORM} ${DISTROLESS_IMAGE} as vcontainer-ccm

COPY --from=certs /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /build/vcontainer-ccm /bin/vcontainer-ccm

LABEL name="vcontainer-ccm" \
      license="Apache Version 2.0" \
      maintainers="Cuong. Duong Manh <cuongdm3@vng.com.vn>" \
      description="vContainer Cloud Controller Manager (aka vContainer-CCM) for vContainer Kubernetes clusters" \
      distribution-scope="public" \
      summary="vContainer Cloud Controller Manager"

CMD [ "/bin/vcontainer-ccm" ]
