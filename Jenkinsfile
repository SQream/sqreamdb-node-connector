pipeline { 
    agent {
            label "x86_64_compilation"
            } 
    stages {
         stage("Set build number and build user"){
            steps {
                wrap([$class: 'BuildUser']){
                script {
                    currentBuild.displayName = "#${BUILD_ID}.${BUILD_USER}"
                }
                }
            }
        }
        stage('git clone nodejs') { 
            steps { 
                sh 'git clone -b $branch http://gitlab.sq.l/connectors/nodejs.git --recursive' 
                }
        }
        stage('Set version and Build'){
            steps {
                sh '''
                cd nodejs
                chmod u+x set_version.sh
                ./set_version.sh
                npm install && npm pack
                '''
            }
        }
        stage('Unit Testing'){
            steps {
                sh 'cd nodejs; node node_modules/.bin/mocha ./test/test_functional.js --require mocha-steps --timeout 4000'               
            }
        }
          stage('upload to artifactory'){
            steps {
                sh 'cd nodejs; curl -u ${ARTIFACT_USER}:${ARTIFACT_PASSWORD} -T *.tgz $ARTIFACTORY_URL/connectors/nodejs/$env/'
                sh 'rm -rf nodejs/'
            }
        }
        }
}