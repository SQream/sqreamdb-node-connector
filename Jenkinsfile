if (params.version_num == "") {
    error("Please provide the version_num parameter!")
}


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
                sh ("""
                rm -rf nodejs/
                git clone -b $branch http://gitlab.sq.l/connectors/nodejs.git --recursive
                """)
                }
        }
        stage('Set version number and Build'){
            steps {
                sh ("""
                cd nodejs
                sed -i "/version/c   \\"version\\" : \\"${version_num}\\"," package.json
                npm install && npm pack
                """)
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