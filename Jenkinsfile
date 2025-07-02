@Library('gbif-common-jenkins-pipelines') _

pipeline {
  agent any
  tools {
    maven 'Maven 3.9.9'
    jdk 'OpenJDK17'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '5'))
    skipStagesAfterUnstable()
    timestamps()
  }
  triggers {
    snapshotDependencies()
  }
  parameters {
    separator(name: "release_separator", sectionHeader: "Release Main Project Parameters")
    booleanParam(name: 'RELEASE', defaultValue: false, description: 'Do a Maven release')
    string(name: 'RELEASE_VERSION', defaultValue: '', description: 'Release version (optional)')
    string(name: 'DEVELOPMENT_VERSION', defaultValue: '', description: 'Development version (optional)')
    booleanParam(name: 'DRY_RUN_RELEASE', defaultValue: false, description: 'Dry Run Maven release')

    separator(name: "release_separator", sectionHeader: "Release Trino UDFs Parameters")
    booleanParam(name: 'RELEASE_TRINO', defaultValue: false, description: 'Do a Maven release')
    string(name: 'RELEASE_VERSION_TRINO', defaultValue: '', description: 'Release version (optional)')
    string(name: 'DEVELOPMENT_VERSION_TRINO', defaultValue: '', description: 'Development version (optional)')
    booleanParam(name: 'DRY_RUN_RELEASE_TRINO', defaultValue: false, description: 'Dry Run Maven release')
  }
  environment {
    JETTY_PORT = utils.getPort()
    POM_VERSION = readMavenPom().getVersion()
  }
  stages {
    stage('Maven build: Main project (Java 17)') {
       when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.RELEASE_TRINO } };
        }
      }
      steps {
        withMaven(
          globalMavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
          mavenOpts: '-Xms2048m -Xmx8192m',
          mavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.MavenSettingsConfig1396361652540',
          traceability: true) {
            configFileProvider([
                configFile(fileId: 'org.jenkinsci.plugins.configfiles.custom.CustomConfig1389220396351', variable: 'APPKEYS_TESTFILE')
              ]) {
              sh 'mvn clean deploy -Denforcer.skip=true -T 1C -Dparallel=classes -DuseUnlimitedThreads=true -Pgbif-dev -U -Djetty.port=${JETTY_PORT} -Dappkeys.testfile=${APPKEYS_TESTFILE}'
            }
          }
      }
    }

    stage('Maven build: Trino module (Java 17)') {
      tools {
        jdk 'OpenJDK17'
      }
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.RELEASE_TRINO } };
        }
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')
          ]) {
          // occurrence-trino-udf needs jdk17 because the trino spi library uses jdk17
          sh '''
            cd occurrence-trino-udf
            mvn -s ${MAVEN_SETTINGS} clean deploy -Pgbif-dev -U -B
          '''
        }
      }
    }

    stage('Build and push Docker images: Downloads') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.RELEASE_TRINO } };
        }
      }
      steps {
         sh '''
          build/occurrence-download-spark-docker-build.sh $POM_VERSION
         '''
      }
    }

    stage('Build and push Docker images: Table build') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.RELEASE_TRINO } };
        }
      }
      steps {
        sh '''
          build/occurrence-table-build-spark-docker-build.sh $POM_VERSION
        '''
      }
    }

    stage('Trigger WS deploy dev') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.RELEASE_TRINO } };
          branch 'dev';
        }
      }
      steps {
        build job: "occurrence-ws-dev-deploy", wait: false, propagate: false
      }
    }

    stage('Maven release: Main project') {
      when {
          allOf {
              expression { params.RELEASE };
              branch 'master';
          }
      }
      environment {
          RELEASE_ARGS = utils.createReleaseArgs(params.RELEASE_VERSION, params.DEVELOPMENT_VERSION, params.DRY_RUN_RELEASE)
      }
      steps {
        withMaven(
          globalMavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
          mavenOpts: '-Xms2048m -Xmx8192m',
          mavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.MavenSettingsConfig1396361652540',
          traceability: true) {
            configFileProvider([
                configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS_XML'),
                configFile(fileId: 'org.jenkinsci.plugins.configfiles.custom.CustomConfig1389220396351', variable: 'APPKEYS_TESTFILE')
              ]) {
              sh 'mvn -s $MAVEN_SETTINGS_XML -B -Denforcer.skip=true release:prepare release:perform $RELEASE_ARGS -Dappkeys.testfile=${APPKEYS_TESTFILE}'
            }
          }
      }
    }

    stage('Maven release: Trino module') {
      tools {
        jdk 'OpenJDK17'
      }
      when {
          allOf {
              expression { params.RELEASE_TRINO };
              branch 'master';
          }
      }
      environment {
          RELEASE_ARGS_TRINO = utils.createReleaseArgs(params.RELEASE_VERSION_TRINO, params.DEVELOPMENT_VERSION_TRINO, params.DRY_RUN_RELEASE_TRINO)
      }
      steps {
          configFileProvider(
                  [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                          variable: 'MAVEN_SETTINGS_XML')]) {
              git 'https://github.com/gbif/occurrence.git'
              sh '''
                cd occurrence-trino-udf
                mvn -s $MAVEN_SETTINGS_XML -B release:prepare release:perform $RELEASE_ARGS_TRINO
              '''
          }
      }
    }

     stage('Docker Release: Downloads') {
       when {
        allOf {
          expression { params.RELEASE };
          not { expression { params.DRY_RUN_RELEASE } }
          branch 'master';
        }
      }
      environment {
          VERSION = utils.getReleaseVersion(params.RELEASE_VERSION, POM_VERSION)
      }
      steps {
        sh 'build/occurrence-download-spark-docker-build.sh ${VERSION}'
      }
     }

     stage('Docker Release: Table build') {
      when {
        allOf {
          expression { params.RELEASE };
          not { expression { params.DRY_RUN_RELEASE } }
          branch 'master';
        }
      }
      environment {
          VERSION = utils.getReleaseVersion(params.RELEASE_VERSION, POM_VERSION)
      }
      steps {
        sh 'build/occurrence-table-build-spark-docker-build.sh ${VERSION}'
      }
     }
  }

    post {
      success {
        echo 'Pipeline executed successfully!'
      }
      failure {
        echo 'Pipeline execution failed!'
    }
  }
}
