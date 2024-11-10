# ucu-mmds
In order to run the pyspark in jupyter notebook locally on your machine
1. Download the docker image
> docker pull jupyter/pyspark-notebook
2. Start the container by mounting the project folder "./mount" to default folder within the container "/home/jovyan/work"
> docker run -it --rm -p 8888:8888 -v ./mount:/home/jovyan/work jupyter/pyspark-notebook
3. Add the localhost:8888 to the jupyter kernels and connect the notebook
![alt text](image.png)

All models fine-tuned during the experiments are copied to the ./local directory