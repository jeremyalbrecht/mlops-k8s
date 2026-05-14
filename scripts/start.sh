minikube start --interactive=false --kubernetes-version=v1.35.1 --cpus=4 --memory=8192 --disk-size=30g
k create ns argocd
k apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/refs/tags/stable/manifests/install.yaml --server-side
k wait --for=condition=available deployment/argocd-applicationset-controller -n argocd --timeout=90s
k apply -f cluster/application-set.yaml