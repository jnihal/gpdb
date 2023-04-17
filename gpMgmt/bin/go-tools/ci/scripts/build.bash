#!/bin/bash
# Copyright (c) 2017-2023 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

set -eux -o pipefail

ccp_src/scripts/setup_ssh_to_cluster.sh

# TODO: ask CCP maintainers for a feature to do this for us
scp cluster_env_files/hostfile_all cdw:/tmp

# Install patchelf. We need to SSH as root, hence the use of
# cluster_env_files.
ssh -t ccp-$(cat cluster_env_files/terraform/name)-0 "sudo bash -c '
    source /home/gpadmin/gpdb_src/concourse/scripts/common.bash
'"

# su - gpadmin bash -c "
#     source /usr/local/greenplum-db/greenplum_path.sh
#     export PATH=/usr/local/go/bin:$PATH
#     cd ${CONTAINER_BASE_DIRECTORY}/gpdb_src/gpMgmt/bin/go-tools
#     make depend-dev
#     make all
# "

ssh -n cdw "
    set -eux -o pipefail

    source /usr/local/greenplum-db-devel/greenplum_path.sh
    export PATH=/usr/local/go/bin:$PATH
    cd /home/gpadmin/gpdb_src/gpMgmt/bin/go-tools
    make depend-dev
    make all
    make install
    source /usr/local/greenplum-db-devel/greenplum_path.sh
    gpsync -f /tmp/hostfile_all /usr/local/greenplum-db-devel/bin/gp =:/usr/local/greenplum-db-devel/bin/gp
    gpssh -f /tmp/hostfile_all mkdir /tmp/certificates
    gpsync -f /tmp/hostfile_all certificates =:/tmp/certificates
    gp install --hostfile /tmp/hostfile_all --server-certificate /tmp/certificates/server-cert.pem --server-key /tmp/certificates/server-key.pem --ca-certificate /tmp/certificates/ca-cert.pem --ca-key /tmp/certificates/ca-key.pem
"
