pipeline {
    agent none

    parameters {
        string(name: 'PYTHON_VERSION', defaultValue: '3.12', description: 'Python version to build for')
        string(name: 'QUICK_TEST', defaultValue: '', description: 'Quick test parameter value (e.g., "1", "2", etc.). Leave empty to skip --quick flag')
        string(name: 'BUILD_ENV_IMAGE', defaultValue: params.BUILD_ENV_IMAGE ?: '', description: 'Docker image for build environment')
        string(name: 'HTTP_PROXY', defaultValue: params.HTTP_PROXY ?: '', description: 'HTTP/HTTPS proxy server')
        string(name: 'NO_PROXY', defaultValue: params.NO_PROXY ?: '', description: 'No proxy hosts')
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
                    // Docker socket group ID
                    env.DOCKER_GID = sh(script: 'stat -c \'%g\' /var/run/docker.sock', returnStdout: true).trim()                    
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
                
                script {
                    // Read version from VERSION file
                    env.VERSION = sh(script: 'cat VERSION', returnStdout: true).trim()
                }
            }
        }
        stage('BUILD') {
            agent {
                docker {
                    alwaysPull false
                    image "${params.BUILD_ENV_IMAGE}"
                    args "-u ${env.JENKINS_UID}:${env.DOCKER_GID} -e HOME=/tmp" +
                         " -e HTTP_PROXY=${params.HTTP_PROXY} -e HTTPS_PROXY=${params.HTTP_PROXY} -e NO_PROXY=${params.NO_PROXY}" +
                         " -e http_proxy=${params.HTTP_PROXY} -e https_proxy=${params.HTTP_PROXY} -e no_proxy=${params.NO_PROXY}" +
                         ' -v /var/run/docker.sock:/var/run/docker.sock --net=host'
                    reuseNode true
                }
            }
            steps {
                sh '''
                    # Convert Python version (3.12 -> 312) for PYVER
                    PYVER=$(echo ${PYTHON_VERSION} | sed 's/\\.//')

                    # Set proxy for cibuildwheel's inner containers
                    export CIBW_ENVIRONMENT_PASS_LINUX="HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy"

                    # Build wheel using cibuildwheel
                    make cibuildwheel PYVER=$PYVER ARCH=x86_64
                '''
                stash name: 'build-artifacts', includes: 'wheelhouse/*.whl'
            }
        }
        stage('TEST') {
            agent {
                docker {
                    alwaysPull false
                    image "python:${params.PYTHON_VERSION}-slim"
                    args "-u ${env.JENKINS_UID}:${env.DOCKER_GID} -e HOME=/tmp" +
                         " -e HTTP_PROXY=${params.HTTP_PROXY} -e HTTPS_PROXY=${params.HTTP_PROXY} -e NO_PROXY=${params.NO_PROXY}" +
                         " -e http_proxy=${params.HTTP_PROXY} -e https_proxy=${params.HTTP_PROXY} -e no_proxy=${params.NO_PROXY}" +
                         ' -v /var/run/docker.sock:/var/run/docker.sock --net=host'
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
                        python ${WORKSPACE}/pyvgx/test/test_pyvgx.py -x --quick=${QUICK_TEST}
                    else
                        python ${WORKSPACE}/pyvgx/test/test_pyvgx.py -x
                    fi
                '''
            }
        }
    }
    post {
        always {
            script {
                currentBuild.displayName = "pyvgx ${env.VERSION}"
                currentBuild.description = "TIMESTAMP: ${env.TIMESTAMP}"
            }
        }
    }    
}
