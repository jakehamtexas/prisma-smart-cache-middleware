type Maybe<T> = T | null;
type ValueOrPromise<T> = T | Promise<T>;
type ValueOf<T> = T[keyof T];

export enum ActionType {
  Cache = 'cache',
  CacheUnique = 'cache-unique',
  Invalidate = 'invalidate',
  InvalidateUnique = 'invalidate-unique',
  Noop = 'noop',
}

export const ActionTypeByName = {
  findFirst: ActionType.Cache,
  findUnique: ActionType.CacheUnique,
  findMany: ActionType.Cache,
  aggregate: ActionType.Cache,
  count: ActionType.Cache,
  findRaw: ActionType.Cache,
  runCommandRaw: ActionType.Noop,
  queryRaw: ActionType.Noop,
  create: ActionType.Invalidate,
  createMany: ActionType.Invalidate,
  update: ActionType.InvalidateUnique,
  updateMany: ActionType.Invalidate,
  upsert: ActionType.InvalidateUnique,
  delete: ActionType.InvalidateUnique,
  deleteMany: ActionType.Invalidate,
  executeRaw: ActionType.Noop,
} as const;

type ActionTypeByName = typeof ActionTypeByName;

export type ActionName = keyof ActionTypeByName;

export type MiddlewareParams<TModelName extends string> = {
  model?: TModelName;
  action: ActionName;
  args: unknown;
  dataPath: string[];
  runInTransaction: boolean;
};

type PrismaMiddleware<TModelName extends string, T = unknown> = (
  params: MiddlewareParams<TModelName>,
  next: (params: MiddlewareParams<TModelName>) => Promise<T>,
) => Promise<T>;

type SelectActionTypeKeys<TType extends ActionType | ActionType[]> = ValueOf<{
  [K in ActionName]: ActionTypeByName[K] extends (TType extends unknown[] ? TType[number] : TType) ? K : never;
}>;
export type UniqueActions = SelectActionTypeKeys<ActionType.InvalidateUnique | ActionType.CacheUnique>;

export type PrismaCacheModelOptions<TDbValue extends object, TModelName extends string> = Partial<
  Record<
    TModelName,
    {
      ttlMs: number;
      uniquenessChecks?: {
        buildUniqueKey: Record<UniqueActions, (params: MiddlewareParams<TModelName>) => Maybe<string>>;
        upsertResultWasUpdate?: (params: MiddlewareParams<TModelName>, value: TDbValue) => boolean;
      };
      cacheKeyPrefix?: string;
    }
  >
>;

export type FullyQualifiedKey<
  TModelName extends string,
  TKey extends string = string,
  TPrefix extends string = never,
> = [TPrefix] extends [never] ? `${TModelName}__${TKey}` : `${TModelName}__${TPrefix}__${TKey}`;

export interface ModelCache<TModelName extends string, TDbValue extends object = object> {
  get(key: FullyQualifiedKey<TModelName>): ValueOrPromise<Maybe<TDbValue>>;
  set(key: FullyQualifiedKey<TModelName>, value: TDbValue, ttlMs: number): ValueOrPromise<void>;
  delete(key: FullyQualifiedKey<TModelName>): ValueOrPromise<void>;
  clear(): ValueOrPromise<void>;
  clearModelKeys(modelName?: TModelName): ValueOrPromise<void>;
}

export type Logger = { log: (...args: string[]) => void };

export type TreatUnknownActionAs<TModelName extends string> = (
  params: MiddlewareParams<TModelName>,
) => Maybe<ActionType>;

type IncludeOrExcludeMethods =
  | {
      excludeMethods?: ActionName[];
    }
  | {
      includeMethods?: ActionName[];
    };

export type PrismaCachingMiddlewareConfig<
  TDbValue extends object,
  TModelName extends string,
> = IncludeOrExcludeMethods & {
  cacheProviderCb: (modelName: TModelName) => ModelCache<TModelName, TDbValue>;
  loggerProviderCb?: () => Logger;
  treatUnknownActionAsCb?: TreatUnknownActionAs<TModelName>;
  modelOptions: PrismaCacheModelOptions<TDbValue, TModelName>;

  // TODO: Make IncludeOrExcludeMethods overridable from model config
  // TODO: Make these model granular as well
  synchronouslyWriteToCache?: boolean;
  synchronouslyDeleteFromCache?: boolean;
};

function hasUnknownAction(params: MiddlewareParams<string>): boolean {
  return !Object.keys(ActionTypeByName).includes(params.action);
}

function makeIsOfTypeFnBy<TModelName extends string = string>(
  treatUnknownActionAsCb: TreatUnknownActionAs<TModelName> | undefined,
): <TType extends ActionType | ActionType[]>(
  type: TType,
) => (
  params: MiddlewareParams<TModelName>,
) => params is Omit<MiddlewareParams<TModelName>, 'action'> & { action: SelectActionTypeKeys<TType> } {
  return <TType extends ActionType | ActionType[]>(type: TType) => {
    const actionNamesOfType = Object.entries(ActionTypeByName)
      .filter(([, actionType]) => (Array.isArray(type) ? type.includes(actionType) : actionType === type))
      .map(([actionName]) => actionName);
    return (
      params: MiddlewareParams<TModelName>,
    ): params is Omit<MiddlewareParams<TModelName>, 'action'> & { action: SelectActionTypeKeys<TType> } =>
      actionNamesOfType.includes(params.action) ||
      (hasUnknownAction(params) && treatUnknownActionAsCb?.(params) === type);
  };
}

function defaultBuildKey(obj: object): string {
  // TODO: Use safe-stable-stringify
  throw new Error('Not implemented');
}

function buildFullyQualifiedKey<TModelName extends string, TKey extends string = string>(
  modelName: TModelName,
  key: TKey,
  prefix: string | undefined,
): FullyQualifiedKey<TModelName, TKey>;
function buildFullyQualifiedKey<TModelName extends string, TKey extends string, TPrefix extends string>(
  modelName: TModelName,
  key: TKey,
  prefix: TPrefix,
): FullyQualifiedKey<TModelName, TKey, TPrefix>;
function buildFullyQualifiedKey<TModelName extends string, TKey extends string, TPrefix extends string>(
  modelName: TModelName,
  key: TKey,
  prefix: TPrefix | undefined,
): FullyQualifiedKey<TModelName, TKey, TPrefix> {
  const withPrefix = prefix && (`${modelName}__${prefix}__${key}` as const);
  const withoutPrefix = `${modelName}__${key}` as const;

  const fullyQualifiedKey = withPrefix ?? withoutPrefix;
  return fullyQualifiedKey as FullyQualifiedKey<TModelName, TKey, TPrefix>;
}

function invoke(fn: () => ValueOrPromise<void>, synchronous: boolean): ValueOrPromise<void> {
  if (synchronous) {
    return fn();
  }
  setImmediate(fn);
}

export function createPrismaCacheMiddleware<TModelName extends string, TDbValue extends object>({
  modelOptions,
  treatUnknownActionAsCb,
  cacheProviderCb,
  loggerProviderCb,
  synchronouslyWriteToCache = true,
  synchronouslyDeleteFromCache = true,
  // TODO: Configure override behavior
  ..._includeOrExcludeMethods
}: PrismaCachingMiddlewareConfig<TDbValue, TModelName>): PrismaMiddleware<TModelName, TDbValue> {
  const makeIsOfTypeFn = makeIsOfTypeFnBy(treatUnknownActionAsCb);
  const [hasAnyCacheAction, hasAnyInvalidateAction, hasNoopAction, hasAnyUniqueAction] = [
    makeIsOfTypeFn([ActionType.Cache, ActionType.CacheUnique]),
    makeIsOfTypeFn([ActionType.Invalidate, ActionType.InvalidateUnique]),
    makeIsOfTypeFn(ActionType.Noop),
    makeIsOfTypeFn([ActionType.CacheUnique, ActionType.InvalidateUnique]),
  ] as const;

  return async (params, next) => {
    const [cache, options] = (params.model && [cacheProviderCb(params.model), modelOptions[params.model]]) ?? [];

    const isUnhandledAction = !Object.values(ActionType)
      .map((actionType) => makeIsOfTypeFn(actionType))
      .some((fn) => fn(params));

    if (isUnhandledAction) {
      const logger = loggerProviderCb?.() ?? console;
      logger.log(
        `Unrecognized Prisma action: ${params.action}. This is potentially unsafe and may mean that cache invalidation is failing!`,
      );
    }

    if (!params.model || !cache || !options || hasNoopAction(params) || isUnhandledAction) {
      return next(params);
    }

    const buildUniqueKey = hasAnyUniqueAction(params) && options.uniquenessChecks?.buildUniqueKey?.[params.action];

    const cacheKey = (buildUniqueKey || defaultBuildKey)(params);

    const fullyQualifiedCacheKey = cacheKey && buildFullyQualifiedKey(params.model, cacheKey, options.cacheKeyPrefix);

    const cached = hasAnyCacheAction(params) && fullyQualifiedCacheKey && (await cache.get(fullyQualifiedCacheKey));
    if (cached) {
      return cached;
    }

    const rv = await next(params);

    if (hasAnyInvalidateAction(params) && cache) {
      const key = buildUniqueKey && buildUniqueKey(params);
      const fullyQualifiedKey = key && buildFullyQualifiedKey(params.model, key, options.cacheKeyPrefix);

      const ifUpsertWasUpdate =
        params.action !== 'upsert' || options.uniquenessChecks?.upsertResultWasUpdate?.(params, rv);

      if (fullyQualifiedKey && ifUpsertWasUpdate) {
        await invoke(() => cache.delete(fullyQualifiedKey), synchronouslyDeleteFromCache);
      } else {
        // If a key cannot be calculated, or the action
        // requires general invalidation (is not Unique),
        // we invalidate the entire model's collection.
        await invoke(() => cache.clearModelKeys(params.model), synchronouslyDeleteFromCache);
      }
    }

    if (fullyQualifiedCacheKey && hasAnyCacheAction(params)) {
      await invoke(() => cache.set(fullyQualifiedCacheKey, rv, options.ttlMs), synchronouslyWriteToCache);
    }

    return rv;
  };
}

// TODO: Tests
// Passthrough when (call next and no cache behavior) when:
//  - is noop action
//  - is unknown action
//  - model options are not defined (model is not included)
//  - method is excluded
//  - method is not included in inclusions override
//
// Reads cache with default key when (calls cache.get with default key) when:
//  - cache method is used
//  - findUnique method is used and no uniqueness cb is given
//
// Reads cache with unique key (calls cache.get with unique key) when:
//  - findUnique method is used
//
// Write cache with default key (calls cache.set with default key, expected value, ttlMs) when:
//  - cache method is used
//  - findUnique method is used and no uniqueness cb is given
//
// Write cache with unique key (calls cache.set with unique key, expected value, ttlMs) when:
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
//
