aws cloudformation update-stack --stack-name transcribe-multi-speaker --template-body file://AudioProcessing_deliverable_CF.yaml --capabilities CAPABILITY_NAMED_IAM 
# cd patch
# zip function.zip lambda_function.py
# aws lambda update-function-code --function-name lambda-function-Audio-post_prosessing-prod --zip-file fileb://function.zip
