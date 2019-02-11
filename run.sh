set -e
set -x

export GOPATH=$(pwd)
export GOBIN=$GOPATH/bin
GO=/usr/lib/go-1.10/bin/go

"$GO" get
"$GO" build
