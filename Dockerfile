FROM gcr.io/distroless/static-debian11:nonroot
ENTRYPOINT ["/baton-google-bigquery"]
COPY baton-google-bigquery /