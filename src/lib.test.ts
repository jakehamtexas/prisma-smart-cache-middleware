import {
  ActionName,
  createPrismaCacheMiddleware,
  MiddlewareParams,
  ModelCache,
  Next,
  PrismaCacheModelOptionsConfig,
} from "./lib";
import { mockDeep } from "jest-mock-extended";

type ModelName = "fake";
const getMockCache = () => mockDeep<ModelCache<ModelName>>();

// TODO: Make tests for exclude/include that use model options granularity
const makeTestCase = <TActionName extends ActionName>({
  fakeModelOptions = { fake: {} },
  excludeAction,
  includeAlternative,
  action = "findUnique" as TActionName,
  description,
}: {
  description: string;
  action?: TActionName;
  fakeModelOptions?: PrismaCacheModelOptionsConfig<object, ModelName>;
  excludeAction?: true;
  includeAlternative?: true;
}) => ({
  action,
  description,
  modelOptions: fakeModelOptions,
  ...(excludeAction ? { excludeActions: [action] } : undefined),
  ...(includeAlternative ? { includeActions: ["aggregate"] } : undefined),
});

it.each([
  makeTestCase({ description: "action is noop action", action: "queryRaw" }),
  makeTestCase({
    description: "action is runCommandRaw",
    action: "runCommandRaw",
  }),
  makeTestCase({ description: "action is executeRaw", action: "executeRaw" }),
  makeTestCase({
    description: "action is unknown",
    action: "foo" as ActionName,
  }),
  makeTestCase({
    description: "model options are not defined",
    fakeModelOptions: {},
  }),
  makeTestCase({
    description: "action is excluded",
    excludeAction: true,
  }),
  makeTestCase({
    description: "action is not included",
    includeAlternative: true,
  }),
] as const)(
  "should not use cache behavior when $description",
  async ({ action, ...config }) => {
    // arrange
    const mockCache = getMockCache();
    const middleware = createPrismaCacheMiddleware({
      cache: { provider: () => mockCache },
      ...config,
    });

    const mockNext: Next<ModelName, object> = jest.fn();
    const expectedParams: MiddlewareParams<ModelName> = {
      action,
      args: undefined,
      dataPath: [],
      runInTransaction: false,
      model: "fake",
    };

    // act
    await middleware(expectedParams, mockNext);

    // assert
    expect(mockNext).toHaveBeenCalledWith(expectedParams);

    expect(mockCache.get).not.toHaveBeenCalled();
    expect(mockCache.set).not.toHaveBeenCalled();
    expect(mockCache.clear).not.toHaveBeenCalled();
    expect(mockCache.clearModelKeys).not.toHaveBeenCalled();
    expect(mockCache.delete).not.toHaveBeenCalled();
  }
);

it.each([
  makeTestCase({
    description: "action is findFirst",
    action: "findFirst",
  }),
  makeTestCase({
    description: "action is findUnique",
    action: "findUnique",
  }),
  makeTestCase({
    description: "action is findMany",
    action: "findMany",
  }),
  makeTestCase({
    description: "action is aggregate",
    action: "aggregate",
  }),
  makeTestCase({
    description: "action is count",
    action: "count",
  }),
  makeTestCase({
    description: "action is findRaw",
    action: "findRaw",
  }),
])(
  "should use cache behavior when $description",
  async ({ action, ...config }) => {
    // arrange
    const mockCache = getMockCache();
    const middleware = createPrismaCacheMiddleware({
      cache: { provider: () => mockCache },
      ...config,
    });

    const mockNext: Next<ModelName, object> = jest.fn();
    const expectedParams: MiddlewareParams<ModelName> = {
      action,
      args: undefined,
      dataPath: [],
      runInTransaction: false,
      model: "fake",
    };

    // act
    await middleware(expectedParams, mockNext);

    // assert
    expect(mockCache.get).toHaveBeenCalled();
  }
);

it("should use unique cache key when findUnique method is used with uniqueness callback", async () => {
  // arrange
  const expectedUniqueKey = "unique";

  const mockCache = getMockCache();
  const middleware = createPrismaCacheMiddleware({
    cache: { provider: () => mockCache },
    modelOptions: {
      fake: {
        uniquenessChecks: {
          buildUniqueKey: {
            findUnique: () => expectedUniqueKey,
          },
        },
      },
    },
  });

  const mockNext: Next<ModelName, object> = jest.fn();
  const expectedParams: MiddlewareParams<ModelName> = {
    action: "findUnique",
    args: undefined,
    dataPath: [],
    runInTransaction: false,
    model: "fake",
  };

  // act
  await middleware(expectedParams, mockNext);

  // assert
  expect(mockCache.get).toHaveBeenCalledWith(
    expect.stringContaining(expectedUniqueKey)
  );
});

// Write cache with default key (calls cache.set with default key, expected value) when:
//  - cache method is used
//  - findUnique method is used and no uniqueness cb is given
//
// Write cache with unique key (calls cache.set with unique key, expected value) when:
//  - findUnique method is used
//
// Delete all models from cache (calls cache.clearModelKeys with model name) when:
//  - invalidate method is used
//  - invalidateUnique method is used with no uniqueness cb given
//  - upsert method is used when op was an insert
//  - upsert method is used when no insert check cb given
//
// Delete unique model from cache (calls cache.delete with unique key) when:
//  - invalidateUnique method is used
//  - upsert method is used when op was an update
//
// Use cache key prefix when:
//  - specified by config
//  - specified by model options
//
// Delete from cache asynchronously when:
//  - specified by config
//  - specified by model options
//  - sync default
//
// Write to cache asynchronously when:
//  - specified by config
//  - specified by model options
//  - sync default
//
// Use logger
// Related models cache invalidation
