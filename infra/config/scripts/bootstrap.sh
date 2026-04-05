#!/bin/sh
# Polaris bootstrap script
# Placeholders: {{REALM}}, {{CREDENTIAL}}

java -jar /deployments/polaris-admin-tool.jar bootstrap -r {{REALM}} -c "{{CREDENTIAL}}" -p
rc=$?
[ $rc -eq 0 ] || [ $rc -eq 3 ] && exit 0 || exit $rc
