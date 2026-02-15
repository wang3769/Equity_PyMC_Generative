pipeline {
  agent any

  environment {
    // ---- Set these in Jenkins "Credentials" and "Global env" ideally ----
    // ACR
    ACR_NAME          = 'YOUR_ACR_NAME'                 // e.g. myregistry
    ACR_LOGIN_SERVER  = "${ACR_NAME}.azurecr.io"
    IMAGE_NAME        = 'equity-dashboard'
    IMAGE_TAG         = "${env.BUILD_NUMBER}"

    // Target VM to run container
    DEPLOY_HOST       = 'YOUR_VM_PUBLIC_IP_OR_DNS'
    DEPLOY_USER       = 'azureuser'
    CONTAINER_NAME    = 'equity-dashboard'
    HOST_PORT         = '8000'
    CONTAINER_PORT    = '8000'

    // Optional: runtime env
    APP_ENV           = 'prod'
  }

  options {
    timestamps()
  }

  stages {
    stage('Checkout') {
      steps { checkout scm }
    }

    stage('Build Docker Image') {
      steps {
        sh """
          docker build -t ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG} .
          docker tag ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG} ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:latest
        """
      }
    }

    stage('Login to ACR') {
      steps {
        withCredentials([usernamePassword(
          credentialsId: 'acr-sp',
          usernameVariable: 'AZ_CLIENT_ID',
          passwordVariable: 'AZ_CLIENT_SECRET'
        )]) {
          // These should also be Jenkins creds or env vars:
          // AZ_TENANT_ID and AZ_SUBSCRIPTION_ID
          sh """
            az login --service-principal -u $AZ_CLIENT_ID -p $AZ_CLIENT_SECRET --tenant $AZ_TENANT_ID
            az account set --subscription $AZ_SUBSCRIPTION_ID
            az acr login --name ${ACR_NAME}
          """
        }
      }
    }

    stage('Push Image to ACR') {
      steps {
        sh """
          docker push ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}
          docker push ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:latest
        """
      }
    }

    stage('Deploy to VM (SSH)') {
      steps {
        sshagent(credentials: ['deploy-vm-ssh']) {
          sh """
            ssh -o StrictHostKeyChecking=no ${DEPLOY_USER}@${DEPLOY_HOST} '
              set -e

              echo "Logging into ACR..."
              docker login ${ACR_LOGIN_SERVER} -u ${ACR_PULL_USER} -p ${ACR_PULL_PASS}

              echo "Pull latest image..."
              docker pull ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:latest

              echo "Stop old container if exists..."
              docker rm -f ${CONTAINER_NAME} || true

              echo "Run new container..."
              docker run -d --restart unless-stopped \
                --name ${CONTAINER_NAME} \
                -p ${HOST_PORT}:${CONTAINER_PORT} \
                -v /opt/equity/data:/app/data \
                -v /opt/equity/docs:/app/docs \
                -e APP_ENV=${APP_ENV} \
                ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:latest

              echo "Prune old images..."
              docker image prune -f
            '
          """
        }
      }
    }
  }

  post {
    always {
      sh "docker system prune -f || true"
    }
  }
}