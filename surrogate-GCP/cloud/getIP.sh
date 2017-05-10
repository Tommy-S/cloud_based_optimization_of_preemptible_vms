
export IP=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attribut\
		 es/startup-script --header "Metadata-Flavor: Google")
