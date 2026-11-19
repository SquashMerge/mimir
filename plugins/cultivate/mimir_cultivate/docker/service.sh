#!/bin/sh
set -eu
install -d -o root -g root -m 0755 /usr/local/libexec
printf '%s\n' '#!/bin/sh' 'umask 022' 'exec /opt/mimir-worklink/venv/bin/python -m mimir_cultivate.worklink.worker_exec "$@"' > /usr/local/libexec/worklink-execd
chmod 0755 /usr/local/libexec/worklink-execd
test -f /opt/cultivate-docker/opencode-enabled
if [ "$(cat /opt/cultivate-docker/worklink-host)" = claude_code ]; then
    rm -rf /etc/s6-overlay/s6-rc.d/worklink-opencode \
        /etc/s6-overlay/s6-rc.d/user/contents.d/worklink-opencode
    rm -f /opt/cultivate-docker/worklink-opencode-service \
        /opt/cultivate-docker/worklink-daemon-gate /opt/cultivate-docker/worklink-opencode-login
else
    install -o root -g root -m 0755 /opt/cultivate-docker/worklink-opencode-service /usr/local/libexec/worklink-opencode-service
    install -o root -g root -m 0755 /opt/cultivate-docker/worklink-daemon-gate /usr/local/libexec/worklink-daemon-gate
    install -o root -g root -m 0755 /opt/cultivate-docker/worklink-opencode-login /usr/local/bin/worklink-opencode-login
    chmod +x /etc/s6-overlay/s6-rc.d/worklink-opencode/run
fi
chmod +x /etc/s6-overlay/s6-rc.d/mimir/run /etc/s6-overlay/s6-rc.d/worklink-execd/run
if [ -f /etc/s6-overlay/s6-rc.d/watchdog/run ]; then
    chmod +x /etc/s6-overlay/s6-rc.d/watchdog/run
fi
