package dev.minisky;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URI;

public final class StorageSmoke {
  private StorageSmoke() {}

  public static void main(String[] args) {
    String endpoint = requiredEnvironment("MINISKY_ENDPOINT");
    URI uri = URI.create(endpoint);
    boolean explicitDockerHost =
        "1".equals(System.getenv("MINISKY_ALLOW_DOCKER_HOST"))
            && "host.docker.internal".equals(uri.getHost());
    if (!"http".equals(uri.getScheme())
        || (!"127.0.0.1".equals(uri.getHost())
            && !"localhost".equals(uri.getHost())
            && !explicitDockerHost)) {
      throw new IllegalArgumentException(
          "MINISKY_ENDPOINT must be loopback or the explicitly enabled Docker host gateway");
    }

    String project = requiredEnvironment("MINISKY_PROJECT_ID");
    String bucketName = requiredEnvironment("MINISKY_JAVA_BUCKET");
    Storage storage =
        StorageOptions.newBuilder()
            .setProjectId(project)
            .setHost(endpoint.replaceAll("/+$", "") + "/_minisky/storage")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();

    Bucket created =
        storage.create(BucketInfo.newBuilder(bucketName).setLocation("US-CENTRAL1").build());
    if (!bucketName.equals(created.getName())) {
      throw new IllegalStateException("created bucket name did not round-trip");
    }
    Bucket observed = storage.get(bucketName);
    if (observed == null || !bucketName.equals(observed.getName())) {
      throw new IllegalStateException("Java SDK could not read the created bucket");
    }
    if (!storage.delete(bucketName) || storage.get(bucketName) != null) {
      throw new IllegalStateException("Java SDK bucket delete did not converge");
    }
    System.out.println("Google Cloud Java Storage create/get/delete smoke passed.");
  }

  private static String requiredEnvironment(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " is required");
    }
    return value;
  }
}
