#!/bin/sh
set -eu

: "${SUPERSET_ADMIN_USERNAME:?SUPERSET_ADMIN_USERNAME is required}"
: "${SUPERSET_ADMIN_PASSWORD:?SUPERSET_ADMIN_PASSWORD is required}"
: "${SUPERSET_ADMIN_EMAIL:?SUPERSET_ADMIN_EMAIL is required}"

superset db upgrade

user_list="$(superset fab list-users)"
if printf '%s\n' "${user_list}" | grep -Fq "username:${SUPERSET_ADMIN_USERNAME} |"; then
    superset fab reset-password \
        --username "${SUPERSET_ADMIN_USERNAME}" \
        --password "${SUPERSET_ADMIN_PASSWORD}"
else
    superset fab create-admin \
        --username "${SUPERSET_ADMIN_USERNAME}" \
        --firstname Olist \
        --lastname Administrator \
        --email "${SUPERSET_ADMIN_EMAIL}" \
        --password "${SUPERSET_ADMIN_PASSWORD}"
fi

superset init

python - <<'PY'
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

source = Path("/opt/lakehouse/assets")
target = Path("/tmp/olist-superset-assets.zip")
with ZipFile(target, "w", ZIP_DEFLATED) as bundle:
    for path in sorted(source.rglob("*.yaml")):
        bundle.write(path, Path("olist-superset-assets") / path.relative_to(source))
PY

python - <<'PY'
import os
from zipfile import ZipFile

from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import security_manager
    from superset.commands.chart.importers.v1 import ImportChartsCommand
    from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
    from superset.commands.dataset.importers.dispatcher import ImportDatasetsCommand
    from superset.commands.importers.v1.utils import get_contents_from_bundle
    from superset.utils.core import override_user

    with ZipFile("/tmp/olist-superset-assets.zip") as bundle:
        contents = get_contents_from_bundle(bundle)
    dataset_contents = {
        **contents,
        "metadata.yaml": "version: 1.0.0\ntype: SqlaTable\n",
    }
    chart_contents = {
        **contents,
        "metadata.yaml": "version: 1.0.0\ntype: Slice\n",
    }

    user = security_manager.find_user(username=os.environ["SUPERSET_ADMIN_USERNAME"])
    with override_user(user=user):
        # Superset 6.1's dashboard importer intentionally imports dependent
        # datasets and charts with overwrite=False. Import each dependency
        # explicitly first so repeated initialization applies changed exports.
        ImportDatasetsCommand(dataset_contents, overwrite=True).run()
        ImportChartsCommand(chart_contents, overwrite=True).run()
        ImportDashboardsCommand(contents, overwrite=True).run()
PY
