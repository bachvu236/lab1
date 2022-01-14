**PLEASE RUN THESE COMMANDS IN GRADLE PROJECT TERMINAL **


export LAB_ID=8
./gradlew clean run -Pargs="
--runner=DataflowRunner \
--gcpTempLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/temp
--stagingLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/staging
--project=nttdata-c4e-bde \
--BQProject=nttdata-c4e-bde \
--BQDataset=uc1_$LAB_ID \
--jobName=usecase1-labid-$LAB_ID \
--pubSubProject=nttdata-c4e-bde \
--subscription=uc1-input-topic-sub-$LAB_ID \
--region=europe-west4 \
--maxNumWorkers=1 \
--workerMachineType=n1-standard-1 \
--serviceAccount=c4e-uc1-sa-$LAB_ID@nttdata-c4e-bde.iam.gserviceaccount.com \
--subnetwork=regions/europe-west4/subnetworks/subnet-uc1-$LAB_ID
--streaming=true"