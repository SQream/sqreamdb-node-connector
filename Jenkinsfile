pipeline { 
    agent {
            label "x86_64_compilation"
            } 
    stages {
        stage('git clone nodejs') { 
            steps { 
                sh 'git clone -b $branch http://gitlab.sq.l/connectors/nodejs.git --recursive' 
                }
        }
        stage('Build'){
            steps {
                sh "cd nodejs; npm install && npm pack"        
            }
        }
        stage('Unit Testing'){
            steps {
                sh 'cd nodejs; node node_modules/.bin/mocha ./test/test_functional.js --require mocha-steps --timeout 4000'               
            }
        }
          stage('upload to artifactory'){
            steps {
                sh 'cd nodejs; curl -u ${ARTIFACT_USER}:${ARTIFACT_PASSWORD} -T *.tgz $ARTIFACTORY_URL/connectors/nodejs/release/'
                sh 'rm -rf nodejs/'
            }
        }
        }
}