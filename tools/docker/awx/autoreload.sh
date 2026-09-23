#!/bin/bash
# restart awx processes when python files in the mounted checkout change
#
# django's autoreload only covers runserver, not uwsgi, daphne, dispatcher & co.

last_reload=0

inotifywait -mrq -e close_write,attrib,create,delete,move --format '%w%f' --exclude '^/awx_devel/awx/ui/|/tests/' /awx_devel/awx | while read -r file; do
  name="$(basename "$file")"
  [[ "$name" == *.py && "$name" != .* ]] || continue

  # one restart per burst of changes (git checkout, editor save dance)
  (( $(date +%s) - last_reload > 1 )) || continue

  echo "autoreload: $file changed, restarting awx processes"
  supervisorctl restart 'tower-processes:*'
  last_reload="$(date +%s)"
done
