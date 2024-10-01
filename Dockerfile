# simple BASE-Image
FROM python:3.10.11

# working directory for te container
WORKDIR /MBCS

# Install requirements
COPY ./requirements.txt  /MBCS/req.txt
RUN pip install --no-cache-dir --upgrade -r /MBCS/req.txt

# Run "API-APP"
COPY ./app /MBCS/app
CMD ["uvicorn", "app.MBCS_API:app", "--host", "0.0.0.0", "--port", "80"]
# NOTE: we might want to change the port-cfg depending on the deployment
#       on the individual k8s-clusters (Arno's cluster, Catena cluster, SFH ...) 