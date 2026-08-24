/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.it.test;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.GenericTableApi;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.TagApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.AssignTagRequest;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UnassignTagRequest;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisTagServiceIntegrationTest {

  private static final String CATALOG_ROLE_1 = "catalogrole1";
  private static final String INVALID_TAG = "INVALID_TAG";
  private static final List<String> ALLOWED_VALUES = List.of("public", "internal");
  private static final List<TargetType> TARGET_TYPES =
      List.of(TargetType.CATALOG, TargetType.TABLE_LIKE);

  private static URI s3BucketBase;
  private static String principalRoleName;
  private static String adminToken;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static TagApi tagApi;

  private String currentCatalogName;
  private RESTCatalog restCatalog;
  private GenericTableApi genericTableApi;

  private static final Namespace NS1 = Namespace.of("NS1");
  private static final TableIdentifier NS1_T1 = TableIdentifier.of(NS1, "T1");

  private final String catalogBaseLocation =
      s3BucketBase + "/" + System.getenv("USER") + "/path/to/data";

  @BeforeAll
  public static void setup(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    String principalName = client.newEntityName("snowman-rest");
    principalRoleName = client.newEntityName("rest-admin");
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    URI testRootUri = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    s3BucketBase = testRootUri.resolve("my-bucket");

    String principalToken = client.obtainToken(principalCredentials);
    tagApi = client.tagApi(principalToken);
  }

  @AfterAll
  public static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    String principalName = "snowman-rest-" + UUID.randomUUID();
    principalRoleName = "rest-admin-" + UUID.randomUUID();
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);

    Method method = testInfo.getTestMethod().orElseThrow();
    currentCatalogName = client.newEntityName(method.getName());
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(catalogBaseLocation))
            .build();

    CatalogProperties.Builder catalogPropsBuilder = CatalogProperties.builder(catalogBaseLocation);
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(currentCatalogName)
            .setProperties(catalogPropsBuilder.build())
            .setStorageConfigInfo(
                s3BucketBase.getScheme().equals("file")
                    ? new FileStorageConfigInfo(
                        StorageConfigInfo.StorageTypeEnum.FILE, List.of(catalogBaseLocation), null)
                    : awsConfigModel)
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    CatalogGrant catalogGrant =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
    managementApi.createCatalogRole(currentCatalogName, CATALOG_ROLE_1);
    managementApi.addGrant(currentCatalogName, CATALOG_ROLE_1, catalogGrant);
    CatalogRole catalogRole = managementApi.getCatalogRole(currentCatalogName, CATALOG_ROLE_1);
    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName, currentCatalogName, catalogRole);

    String principalToken = client.obtainToken(principalCredentials);
    tagApi = client.tagApi(principalToken);
    genericTableApi = client.genericTableApi(principalToken);
    restCatalog =
        IcebergHelper.restCatalog(endpoints, currentCatalogName, Map.of(), principalToken);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } finally {
      client.cleanUp(adminToken);
    }
  }

  private Tag createDefaultTag(String name) {
    return tagApi.createTag(currentCatalogName, name, "a comment", ALLOWED_VALUES, TARGET_TYPES);
  }

  @Test
  public void testCreateTag() {
    Tag tag = createDefaultTag("classification");
    Assertions.assertThat(tag.getName()).isEqualTo("classification");
    Assertions.assertThat(tag.getComment()).isEqualTo("a comment");
    Assertions.assertThat(tag.getAllowedValues()).containsExactlyInAnyOrder("public", "internal");
    Assertions.assertThat(tag.getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.TABLE_LIKE);
    Assertions.assertThat(tag.getVersion()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"tag.name", "tag name", "tag/name", "tag!", "标签"})
  public void testCreateTagWithInvalidName(String invalidName) {
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\""
                        + invalidName
                        + "\",\"allowed-values\":[\"a\"],\"target-types\":[\"catalog\"]}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }
  }

  @Test
  public void testCreateTagDuplicate() {
    createDefaultTag("classification");
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"classification\",\"allowed-values\":[\"a\"],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      Assertions.assertThat(body).contains("Tag already exists");
      Assertions.assertThat(body).contains("AlreadyExistsException");
    }
  }

  @Test
  public void testCreateTagInvalidAllowedValues() {
    // Empty list: the schema declares no minItems for allowed-values, so this is a server-side
    // rule, not a bean-validation one.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t1\",\"allowed-values\":[],\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Empty member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t2\",\"allowed-values\":[\"a\",\"\"],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Duplicate member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t3\",\"allowed-values\":[\"a\",\"a\"],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Null member: a validation 400 with the documented type, never a mapping failure.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t4\",\"allowed-values\":[\"a\",null],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
  }

  @Test
  public void testCreateTagInvalidTargetTypes() {
    // Empty list.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json("{\"name\":\"t1\",\"allowed-values\":[\"a\"],\"target-types\":[]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Unknown member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t2\",\"allowed-values\":[\"a\"],"
                        + "\"target-types\":[\"warehouse\"]}"))) {
      // An unknown member deserializes to null (server-side deserializer) and is rejected by
      // validation with the documented error type, same as any other invalid list.
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Null member: deserializes into the list (nothing rejects it at the schema layer), so the
    // server-side validation must catch it as a 400 instead of a mapping failure.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t4\",\"allowed-values\":[\"a\"],"
                        + "\"target-types\":[\"catalog\",null]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Duplicate member: the schema declares no uniqueItems, so this is a server-side rule.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t3\",\"allowed-values\":[\"a\"],"
                        + "\"target-types\":[\"catalog\",\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
  }

  @Test
  public void testLoadTag() {
    createDefaultTag("classification");
    Tag tag = tagApi.loadTag(currentCatalogName, "classification");
    Assertions.assertThat(tag.getName()).isEqualTo("classification");
    Assertions.assertThat(tag.getVersion()).isEqualTo(0);
  }

  @Test
  public void testLoadNonExistingTag() {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", INVALID_TAG))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      String body = res.readEntity(String.class);
      Assertions.assertThat(body).contains("Tag does not exist: " + INVALID_TAG);
      Assertions.assertThat(body).contains("NoSuchTagException");
    }
  }

  @Test
  public void testListTags() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    List<TagIdentifier> identifiers = tagApi.listTags(currentCatalogName);
    Assertions.assertThat(identifiers)
        .extracting(TagIdentifier::getName)
        .containsExactlyInAnyOrder("classification", "sensitivity");
  }

  @Test
  public void testListTagsEmpty() {
    Assertions.assertThat(tagApi.listTags(currentCatalogName)).isEmpty();
  }

  @Test
  public void testUpdateTag() {
    createDefaultTag("classification");
    UpdateTagRequest request =
        UpdateTagRequest.builder()
            .setComment("updated comment")
            .setAllowedValues(List.of("public"))
            .setCurrentTagVersion(0)
            .build();
    Tag updated = tagApi.updateTag(currentCatalogName, "classification", request);
    Assertions.assertThat(updated.getComment()).isEqualTo("updated comment");
    Assertions.assertThat(updated.getAllowedValues()).containsExactly("public");
    Assertions.assertThat(updated.getVersion()).isEqualTo(1);
    // target-types is create-only: the update request declares the field as nullable, and any
    // non-null value, including an empty list, is rejected with 400 (null means omitted).
    Assertions.assertThat(updated.getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.TABLE_LIKE);
  }

  @Test
  public void testUpdateTagVersionMismatch() {
    createDefaultTag("classification");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(Entity.json("{\"comment\":\"x\",\"current-tag-version\":7}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.CONFLICT.getStatusCode());
      String body = res.readEntity(String.class);
      Assertions.assertThat(body).contains("Tag version mismatch");
      Assertions.assertThat(body).contains("TagVersionMismatchException");
    }
  }

  @Test
  public void testUpdateNonExistingTag() {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", INVALID_TAG))
            .put(Entity.json("{\"current-tag-version\":0}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testUpdateTagRename() {
    createDefaultTag("classification");
    UpdateTagRequest request =
        UpdateTagRequest.builder().setName("category").setCurrentTagVersion(0).build();
    Tag renamed = tagApi.updateTag(currentCatalogName, "classification", request);
    Assertions.assertThat(renamed.getName()).isEqualTo("category");
    Assertions.assertThat(renamed.getVersion()).isEqualTo(1);

    Tag loaded = tagApi.loadTag(currentCatalogName, "category");
    Assertions.assertThat(loaded.getName()).isEqualTo("category");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testUpdateTagRenameCollision() {
    createDefaultTag("classification");
    createDefaultTag("category");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}", Map.of("cat", currentCatalogName, "tag", "category"))
            .put(Entity.json("{\"name\":\"classification\",\"current-tag-version\":0}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      Assertions.assertThat(body).contains("Tag already exists");
      Assertions.assertThat(body).contains("AlreadyExistsException");
    }
  }

  @Test
  public void testCreateTagAllowedValuesOrderPreserved() {
    // The spec promises the submitted allowed-values order is preserved for display; assert it
    // wire-to-wire with a deliberately unsorted list.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"ordertag\",\"allowed-values\":[\"b\",\"c\",\"a\"],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}", Map.of("cat", currentCatalogName, "tag", "ordertag"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(body.indexOf("\"b\"")).as(body).isPositive();
      Assertions.assertThat(body.indexOf("\"b\"")).as(body).isLessThan(body.indexOf("\"c\""));
      Assertions.assertThat(body.indexOf("\"c\"")).as(body).isLessThan(body.indexOf("\"a\""));
    }
    Tag loaded = tagApi.loadTag(currentCatalogName, "ordertag");
    Assertions.assertThat(loaded.getAllowedValues()).containsExactly("b", "c", "a");
  }

  @Test
  public void testUpdateTagOmittedFieldsUnchanged() {
    createDefaultTag("classification");
    // Only the required version field is supplied; every mutable field is omitted. Nothing
    // changes, so the update short-circuits and the version does not advance.
    Tag updated =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder().setCurrentTagVersion(0).build());
    Assertions.assertThat(updated.getName()).isEqualTo("classification");
    Assertions.assertThat(updated.getComment()).isEqualTo("a comment");
    Assertions.assertThat(updated.getAllowedValues())
        .containsExactlyInAnyOrder("public", "internal");
    Assertions.assertThat(updated.getVersion()).isEqualTo(0);
  }

  @Test
  public void testUpdateTagExplicitNullIgnored() {
    createDefaultTag("classification");
    // For the two nullable lists, allowed-values and target-types, an explicit JSON null is the
    // same as omitting the field: nothing changes, the update short-circuits, and the version
    // does not advance.
    for (String json :
        List.of(
            "{\"allowed-values\":null,\"current-tag-version\":0}",
            "{\"target-types\":null,\"current-tag-version\":0}")) {
      try (Response res =
          tagApi
              .request(
                  "polaris/v1/{cat}/tags/{tag}",
                  Map.of("cat", currentCatalogName, "tag", "classification"))
              .put(Entity.json(json))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(body)
            .isEqualTo(Response.Status.OK.getStatusCode());
      }
    }
    // Nothing changed along the way.
    Tag tag = tagApi.loadTag(currentCatalogName, "classification");
    Assertions.assertThat(tag.getName()).isEqualTo("classification");
    Assertions.assertThat(tag.getComment()).isEqualTo("a comment");
    Assertions.assertThat(tag.getAllowedValues()).containsExactlyInAnyOrder("public", "internal");
    Assertions.assertThat(tag.getVersion()).isEqualTo(0);
  }

  @Test
  public void testUpdateTagEmptyCommentClears() {
    createDefaultTag("classification");
    // An explicit empty string is the supported way to clear a comment: unlike null it is
    // unambiguous on the wire. Everything else stays untouched.
    Tag updated =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder().setComment("").setCurrentTagVersion(0).build());
    Assertions.assertThat(updated.getComment()).isEmpty();
    Assertions.assertThat(updated.getName()).isEqualTo("classification");
    Assertions.assertThat(updated.getAllowedValues()).containsExactly("public", "internal");
    Assertions.assertThat(updated.getVersion()).isEqualTo(1);
  }

  @Test
  public void testUpdateTagTargetTypesRejected() {
    createDefaultTag("classification");
    // target-types is create-only; any non-null value, including an empty list, is rejected,
    // matching the error the API's own catalog documents for updateTag.
    for (String json :
        List.of(
            "{\"target-types\":[\"catalog\"],\"current-tag-version\":0}",
            "{\"target-types\":[],\"current-tag-version\":0}")) {
      try (Response res =
          tagApi
              .request(
                  "polaris/v1/{cat}/tags/{tag}",
                  Map.of("cat", currentCatalogName, "tag", "classification"))
              .put(Entity.json(json))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        Assertions.assertThat(body).contains("create-only");
        Assertions.assertThat(body).contains("BadRequestException");
      }
    }
  }

  @Test
  public void testUpdateTagEmptyAllowedValuesRejected() {
    createDefaultTag("classification");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(Entity.json("{\"allowed-values\":[],\"current-tag-version\":0}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("at least one value");
      Assertions.assertThat(body).contains("BadRequestException");
    }
  }

  @Test
  public void testUpdateTagNullListMemberRejected() {
    createDefaultTag("classification");
    // A null member inside a present list is a validation 400 on update too, never a mapping
    // failure; a target-types list with a null member is still a non-null value, so it hits the
    // create-only rejection.
    for (String json :
        List.of(
            "{\"allowed-values\":[\"a\",null],\"current-tag-version\":0}",
            "{\"target-types\":[null],\"current-tag-version\":0}")) {
      try (Response res =
          tagApi
              .request(
                  "polaris/v1/{cat}/tags/{tag}",
                  Map.of("cat", currentCatalogName, "tag", "classification"))
              .put(Entity.json(json))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        Assertions.assertThat(body).contains("BadRequestException");
      }
    }
  }

  @Test
  public void testDropTag() {
    createDefaultTag("classification");
    tagApi.dropTag(currentCatalogName, "classification");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testDropNonExistingTag() {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", INVALID_TAG))
            .delete()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testCatalogDropBlockedByTag() {
    createDefaultTag("classification");
    // A live tag definition blocks dropping its catalog, the same way live namespaces do:
    // dropping the catalog would leave the tag behind as an unreachable row.
    try (Response res =
        managementApi.request("v1/catalogs/{cat}", Map.of("cat", currentCatalogName)).delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("not empty");
    }
    // Dropping the tag unblocks the catalog drop (dropCatalog also removes the test's extra
    // catalog role, which would otherwise block deletion on its own).
    tagApi.dropTag(currentCatalogName, "classification");
    managementApi.dropCatalog(currentCatalogName);
  }

  @Test
  public void testDropTagDetachAll() {
    createDefaultTag("classification");
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    tagApi.assignTag(currentCatalogName, "classification", catalogTarget, List.of("public"));

    // a plain drop refuses while assignments remain and changes nothing
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"),
                Map.of("detach-all", "false"))
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("in use");
      Assertions.assertThat(body).contains("TagInUseException");
    }
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification")).isNotNull();

    // detach-all removes every assignment and the definition together
    tagApi.dropTag(currentCatalogName, "classification", true);
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  private Tag createAllTargetsTag(String name) {
    return tagApi.createTag(
        currentCatalogName,
        name,
        "a comment",
        ALLOWED_VALUES,
        List.of(
            TargetType.CATALOG, TargetType.NAMESPACE, TargetType.TABLE_LIKE, TargetType.COLUMN));
  }

  private TagAttachmentTarget tableTarget() {
    return TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
        .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
        .build();
  }

  private void createT1() {
    restCatalog.createNamespace(NS1);
    restCatalog
        .buildTable(
            NS1_T1,
            new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get())))
        .create();
  }

  @Test
  public void testAssignAndUnassignTag() {
    createAllTargetsTag("assigntag");
    createT1();

    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    TagAttachmentTarget namespaceTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS1.levels()[0])).build();

    tagApi.assignTag(currentCatalogName, "assigntag", catalogTarget, List.of("public"));
    // re-assigning the same identity replaces the value
    tagApi.assignTag(currentCatalogName, "assigntag", catalogTarget, List.of("internal"));
    tagApi.assignTag(currentCatalogName, "assigntag", namespaceTarget, List.of("public"));
    tagApi.assignTag(currentCatalogName, "assigntag", tableTarget(), List.of("public"));

    tagApi.unassignTag(currentCatalogName, "assigntag", catalogTarget);
    tagApi.unassignTag(currentCatalogName, "assigntag", namespaceTarget);
    tagApi.unassignTag(currentCatalogName, "assigntag", tableTarget());

    // unassigning a missing relationship is a 404
    UnassignTagRequest unassignRequest =
        UnassignTagRequest.builder().setTarget(catalogTarget).build();
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", "assigntag"))
            .post(Entity.json(unassignRequest))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains("NoSuchMappingException");
    }
  }

  @Test
  public void testAssignTagToColumn() {
    createAllTargetsTag("coltag");
    createT1();
    TagAttachmentTarget columnTarget =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build();
    tagApi.assignTag(currentCatalogName, "coltag", columnTarget, List.of("public"));
    tagApi.unassignTag(currentCatalogName, "coltag", columnTarget);

    // a column absent from the current schema is a 404
    TagAttachmentTarget badColumn =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("nope"))
            .build();
    assertAssignFails(
        "coltag", badColumn, List.of("public"), Response.Status.NOT_FOUND, "NoSuchTargetException");

    // a present-but-empty or multi-segment column member is malformed, not a miss
    assertAssignFails(
        "coltag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of(""))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    assertAssignFails(
        "coltag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data", "nested"))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // an absent column list is malformed too, not merely unresolvable
    assertAssignFails(
        "coltag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // a null column member inside a present list is malformed as well, never a server error
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", "coltag"))
            .put(
                Entity.json(
                    "{\"target\":{\"type\":\"column\",\"path\":[\"NS1\",\"T1\"],"
                        + "\"column\":[null]},\"values\":[\"public\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
  }

  @Test
  public void testTagValueByteBound() {
    // A value of exactly the bound is accepted end to end (definition, assignment, read back);
    // one byte more is rejected at both the definition and the assignment with a message naming
    // the limit.
    String atLimit = "v".repeat(2000);
    String overLimit = "v".repeat(2001);
    tagApi.createTag(
        currentCatalogName,
        "boundtag",
        null,
        List.of(atLimit, "short"),
        List.of(TargetType.CATALOG));
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "boundtag").getAllowedValues())
        .contains(atLimit);
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    tagApi.assignTag(currentCatalogName, "boundtag", catalogTarget, List.of(atLimit));
    tagApi.unassignTag(currentCatalogName, "boundtag", catalogTarget);

    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"overtag\",\"allowed-values\":[\""
                        + overLimit
                        + "\"],\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("2000");
    }
    assertAssignFails(
        "boundtag",
        catalogTarget,
        List.of(overLimit),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
  }

  @Test
  public void testAssignTagValueAndTargetValidation() {
    // definition allows only catalog targets
    tagApi.createTag(
        currentCatalogName, "narrowtag", null, ALLOWED_VALUES, List.of(TargetType.CATALOG));
    createT1();
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();

    // value outside the current allowed values
    assertAssignFails(
        "narrowtag",
        catalogTarget,
        List.of("restricted"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // empty and multi-value lists
    assertAssignFails(
        "narrowtag", catalogTarget, List.of(), Response.Status.BAD_REQUEST, "BadRequestException");
    assertAssignFails(
        "narrowtag",
        catalogTarget,
        List.of("public", "internal"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // an empty-string value member is malformed, not merely outside the allowed values
    assertAssignFails(
        "narrowtag",
        catalogTarget,
        List.of(""),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // a supported target kind the definition does not list: 400 on an existing target, but the
    // target is resolved first, so the same excluded kind on a missing target answers the
    // target-level 404
    assertAssignFails(
        "narrowtag",
        tableTarget(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(List.of(NS1.levels()[0], "missing_for_excluded_kind"))
            .build(),
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTargetException");
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("no_such_column"))
            .build(),
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTargetException");
    // a catalog target must not carry a path
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.CATALOG).setPath(List.of(NS1.levels()[0])).build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // malformed target shapes: empty namespace path, table path without a table segment,
    // and requests missing the target or its type entirely
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of()).build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(List.of(NS1.levels()[0]))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", "narrowtag"))
            .put(Entity.json("{\"values\":[\"public\"]}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", "narrowtag"))
            .put(Entity.json("{\"target\":{\"path\":[\"ns\"]},\"values\":[\"public\"]}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }

    // A target whose path does not resolve answers the target-level 404 naming the missing
    // entity (table or namespace), and that classification wins over a missing tag. Column
    // misses are detected later, after the tag lookup: a resolvable table with an absent column
    // answers the column-level 404 (asserted in testAssignTagToColumn), but a missing tag wins
    // over a missing column.
    TagAttachmentTarget missingTable =
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(List.of(NS1.levels()[0], "missing"))
            .build();
    assertAssignFails(
        "narrowtag",
        missingTable,
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTargetException");
    TagAttachmentTarget missingNamespace =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of("no_such_ns")).build();
    assertAssignFails(
        "narrowtag",
        missingNamespace,
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTargetException");
    // a tag that does not exist, and both misses at once: the target-level 404 wins because
    // target resolution fails the request before the tag lookup runs
    assertAssignFails(
        "missingtag",
        catalogTarget,
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTagException");
    assertAssignFails(
        "missingtag",
        missingTable,
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTargetException");
    // unassign classifies a missing target the same way
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", "narrowtag"))
            .post(Entity.json(UnassignTagRequest.builder().setTarget(missingTable).build()))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(body).contains("NoSuchTargetException");
    }

    // grandfathering: narrow the list after assigning, the write with the removed value fails
    tagApi.assignTag(currentCatalogName, "narrowtag", catalogTarget, List.of("internal"));
    Tag current = tagApi.loadTag(currentCatalogName, "narrowtag");
    tagApi.updateTag(
        currentCatalogName,
        "narrowtag",
        UpdateTagRequest.builder()
            .setCurrentTagVersion(current.getVersion())
            .setAllowedValues(List.of("public"))
            .build());
    assertAssignFails(
        "narrowtag",
        catalogTarget,
        List.of("internal"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");

    // Remove the still-assigned tag explicitly so later tests' cleanup starts from a clean
    // catalog.
    tagApi.dropTag(currentCatalogName, "narrowtag", true);
  }

  @Test
  public void testAssignTagToViewAndGenericTableColumnRejected() {
    createAllTargetsTag("subtypetag");
    restCatalog.createNamespace(NS1);

    // v1 excludes Iceberg views as targets entirely, whole-object assignment included
    TableIdentifier viewId = TableIdentifier.of(NS1, "V1");
    restCatalog
        .buildView(viewId)
        .withSchema(new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
        .withDefaultNamespace(NS1)
        .withQuery("spark", "select 1 as id")
        .create();
    assertAssignFails(
        "subtypetag",
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(viewId))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");

    // a generic table defines no stable column id, so column targets are rejected on it
    TableIdentifier genericId = TableIdentifier.of(NS1, "G1");
    GenericTable genericTable =
        genericTableApi.createGenericTable(currentCatalogName, genericId, "format", Map.of());
    Assertions.assertThat(genericTable).isNotNull();
    assertAssignFails(
        "subtypetag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(genericId))
            .setColumn(List.of("c1"))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    genericTableApi.purge(currentCatalogName, NS1);
  }

  @Test
  public void testUnassignTagMalformedTargetRejected() {
    createAllTargetsTag("unassigntag");
    createT1();

    // an empty namespace path
    assertUnassignFails(
        "unassigntag",
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of()).build(),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // a table-like path must name a namespace and a table, not just the namespace
    assertUnassignFails(
        "unassigntag",
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(List.of(NS1.levels()[0]))
            .build(),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // a column target must name exactly one top-level column, not two
    assertUnassignFails(
        "unassigntag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data", "nested"))
            .build(),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
    // column is only valid for column targets, not for a table-like target
    assertUnassignFails(
        "unassigntag",
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build(),
        Response.Status.BAD_REQUEST,
        "BadRequestException");
  }

  private void assertAssignFails(
      String tagName,
      TagAttachmentTarget target,
      List<String> values,
      Response.Status expected,
      String expectedType) {
    AssignTagRequest request =
        AssignTagRequest.builder().setTarget(target).setValues(List.copyOf(values)).build();
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", tagName))
            .put(Entity.json(request))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(expected.getStatusCode());
      Assertions.assertThat(body).contains(expectedType);
    }
  }

  private void assertUnassignFails(
      String tagName, TagAttachmentTarget target, Response.Status expected, String expectedType) {
    UnassignTagRequest request = UnassignTagRequest.builder().setTarget(target).build();
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", tagName))
            .post(Entity.json(request))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(expected.getStatusCode());
      Assertions.assertThat(body).contains(expectedType);
    }
  }
}
