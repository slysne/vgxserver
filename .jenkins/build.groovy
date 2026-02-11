pipeline {
    agent none

    parameters {
        string(name: 'PYTHON_VERSION', defaultValue: '3.12', description: 'Python version to build for')
        string(name: 'QUICK_TEST', defaultValue: '', description: 'Quick test parameter value (e.g., "1", "2", etc.). Leave empty to skip --quick flag')
    }

    environment {
        // Gsp docker nexus repository
        GSP_DOCKER_PULL = 'gsp-docker.intra.rakuten-it.com'
    }

    stages {
        stage('PREPARE ENV') {
            agent any
            steps {
                // Clean workspace
                cleanWs()
                script {
                    // Current jenkins workspace
                    env.WORKSPACE = pwd()
                    // Current Timestamp
                    env.TIMESTAMP = sh(script: 'date -u "+%Y%m%d.%H%M%S"', returnStdout: true).trim()
                    // Current jenkins server user ID
                    env.JENKINS_UID = sh(script: 'id -u', returnStdout: true).trim()
                    // Current jenkins server group ID
                    env.JENKINS_GID = sh(script: 'id -g', returnStdout: true).trim()
                    // Read version from VERSION file
                    env.VERSION = sh(script: 'cat VERSION', returnStdout: true).trim()
                    // Jenkins script SCM credential and URL
                    env.SCM_BRANCH = scm.branches[0].name
                    env.SCM_URL = scm.getUserRemoteConfigs()[0].getUrl()
                    env.SCM_CRED = scm.getUserRemoteConfigs()[0].getCredentialsId()
                }
                checkout changelog: false,
                        poll: false,
                        scm: [$class                           : 'GitSCM',
                              branches                         : [[name: env.SCM_BRANCH]],
                              doGenerateSubmoduleConfigurations: false,
                              extensions                       : [[$class: 'LocalBranch', localBranch: '**'], [$class: 'CleanBeforeCheckout'],
                                                                  [$class           : 'SubmoduleOption', disableSubmodules: false,
                                                                   parentCredentials: true, recursiveSubmodules: true,
                                                                   reference        : '', trackingSubmodules: true]],
                              submoduleCfg                     : [],
                              userRemoteConfigs                : [[credentialsId: env.SCM_CRED, url: env.SCM_URL]]]
            }
            post {
                always {
                    // Reset workspace permission
                    withDockerContainer(args: "-v ${env.WORKSPACE}:/home/tmp:rw,z", image: "${GSP_DOCKER_PULL}/busybox:1.31.0") {
                        sh "chown -R ${JENKINS_UID}:${JENKINS_GID} /home/tmp/"
                    }
                }
            }
        }
        stage('BUILD') {
            agent {
                docker {
                    alwaysPull false
                    image "${env.GSP_DOCKER_PULL}/python:${params.PYTHON_VERSION}-slim"
                    args '-u root:root' +
                         " -v /var/run/docker.sock:/var/run/docker.sock --net=host"
                    reuseNode true
                }
            }
            steps {
                sh '''
                    # Install minimal dependencies (cibuildwheel uses its own build containers)
                    apt-get update && apt-get install -y make
                    pip install --upgrade pip cibuildwheel

                    # Convert Python version (3.12 -> 312) for PYVER
                    PYVER=$(echo ${PYTHON_VERSION} | sed 's/\\.//')

                    # Build wheel using cibuildwheel
                    make cibuildwheel PYVER=$PYVER ARCH=x86_64
                '''
                stash name: 'build-artifacts', includes: 'wheelhouse/*.whl'
            }
            post {
                always {
                    // Reset workspace permission
                    withDockerContainer(args: "-v ${env.WORKSPACE}:/home/tmp:rw,z", image: "${GSP_DOCKER_PULL}/busybox:1.31.0") {
                        sh "chown -R ${JENKINS_UID}:${JENKINS_GID} /home/tmp/"
                    }
                }
            }            
        }
        stage('TEST') {
            agent {
                docker {
                    alwaysPull false
                    image "${env.GSP_DOCKER_PULL}/python:${params.PYTHON_VERSION}-slim"
                    args '-u root:root' +
                         " -v /var/run/docker.sock:/var/run/docker.sock --net=host"
                    reuseNode true
                }
            }
            steps {
                unstash 'build-artifacts'
                sh '''
                    # Install the built wheel package
                    pip install wheelhouse/*.whl

                    # Run tests with optional --quick parameter
                    if [ -n "${QUICK_TEST}" ]; then
                        python test_pyvgx.py -x --quick=${QUICK_TEST}
                    else
                        python test_pyvgx.py -x
                    fi
                '''
            }
            post {
                always {
                    // Reset workspace permission
                    withDockerContainer(args: "-v ${env.WORKSPACE}:/home/tmp:rw,z", image: "${GSP_DOCKER_PULL}/busybox:1.31.0") {
                        sh "chown -R ${JENKINS_UID}:${JENKINS_GID} /home/tmp/"
                    }
                }
            }
        }
    }
    post {
        always {
            script {
                currentBuild.displayName = "pyvgx ${env.VERSION} - Python ${params.PYTHON_VERSION}"
                currentBuild.description = "TIMESTAMP: ${env.TIMESTAMP}"
            }
        }
    }    
}
