# EC2-CloudWatch connector for HIRO

**Status**

- This is currently a preview, though we already use it in production.
- Currently documentation and installation parts are missing
- The project can be built using maven


**Components**
Connector uses following AWS services https://aws.amazon.com/cloudwatch/ and https://aws.amazon.com/sqs/ to do following:
1. Transforms CloudWatch Alarms into AutomationIssue.
2. Read CloudWatch Metrics and save as Timeseries.

**Requirements**
- access to AWS console
- access to WSO2 console
- could be installed on ec2 machine or any other local machine(requires AWS credentials)
- MARS Model in HIRO for AWS infrastructure which should be connected to

**Installation && Configuration**
//TODO

**Contributing**

We value contribution, if you have any changes or suggestions, or if you have created a new action handler, please feel free to open a pull request add it here.

If you submit content to this repository, it will be licensed under the MIT license (see below).

**Questions and Support**

For questions and support, please refer to our documentation https://docs.hiro.arago.co/ and visit our community https://hiro.community/ for asking detailed questions.

**License (MIT)**

Copyright (c) 2017 arago GmbH

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
