{-# OPTIONS_GHC -fno-warn-name-shadowing #-}

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TupleSections #-}

module AutoFailover where

import Control.Monad (forM, replicateM)
import Control.Monad.State (State, StateT,
                            evalState, evalStateT,
                            runState, execState, gets, modify)
import Control.Monad.Trans (lift)
import Data.List ((\\), sort, delete, nub)
import Data.Maybe (catMaybes)
import GHC.Generics (Generic)
import Text.PrettyPrint.Generic (Pretty, prettyShow)

import Text.Printf (printf)
import System.Process (readProcess)
import Test.QuickCheck (Arbitrary(arbitrary, shrink),
                        Gen, Property, (===),
                        forAll, resize, sized, choose, elements,
                        ioProperty)

type NodeId = Int
data DownState = New | HalfDown | NearlyDown | Failover | Up
               deriving (Eq, Generic, Pretty)

instance Show DownState where
  show = prettyShow

data NodeState = NodeState { name              :: NodeId
                           , downCounter       :: Int
                           , downState         :: DownState
                           , mailedDownWarning :: Bool
                           }
               deriving (Eq, Generic, Pretty)

instance Show NodeState where
  show = prettyShow

data AFState = AFState { nodeStates            :: [NodeState]
                       , mailedTooSmallCluster :: [NodeId]
                       , downThreshold         :: Int
                       }
             deriving (Generic, Pretty)

instance Show AFState where
  show = prettyShow

type AF = State AFState

evalAF :: AF a -> AFState -> a
evalAF = evalState

runAF :: AF a -> AFState -> (a, AFState)
runAF = runState

execAF :: AF a -> AFState -> AFState
execAF = execState

data ActionType = DoFailover
                | DoMailTooSmall
                | DoMailDownWarning
                deriving (Eq, Ord, Generic, Pretty)

type Action = (ActionType, NodeId)

actionType :: Action -> ActionType
actionType = fst

actionNode :: Action -> NodeId
actionNode = snd

instance Show ActionType where
  show = prettyShow

failoverThreshold :: Int
failoverThreshold = 6

downGracePeriod :: Int
downGracePeriod = 2

minClusterSize :: Int
minClusterSize = 2

defaultState :: AFState
defaultState =
  AFState { nodeStates            = []
          , mailedTooSmallCluster = []
          , downThreshold         = failoverThreshold - 1 - downGracePeriod
          }

processFrame :: [NodeId] -> [NodeId] -> AF [Action]
processFrame allNodes downNodes = do
  currentNodes <- getNodes
  let removedNodes = currentNodes \\ allNodes
  modifyNodeStates $ filter (not . oneOf removedNodes)

  let newNodes = allNodes \\ currentNodes
  modifyNodeStates (map mkNewNodeState newNodes ++)

  modifyNodeStates $ map $ choose (oneOf upNodes) processUp id

  let nodesChanged = not $ null newNodes && null removedNodes
  modifyNodeStatesM $ mapM $
    choose (oneOf downNodes) (processDown nodesChanged) return

  getActions

  where upNodes = allNodes \\ downNodes

        choose pred f g x | pred x    = f x
                          | otherwise = g x

        oneOf nodes ns = name ns `elem` nodes

        mkNewNodeState node = NodeState { name              = node
                                        , downCounter       = 0
                                        , downState         = New
                                        , mailedDownWarning = False
                                        }

        processUp ns = ns { downState = Up
                          , downCounter = 0
                          , mailedDownWarning = False
                          }

        processDown nodesChanged ns =
          doProcessDown nodesChanged <$> gets downThreshold <*> pure ns

        doProcessDown nodesChanged downThreshold ns
          | state == New || state == Up = ns { downState = HalfDown }
          | nodesChanged                = ns { downState = HalfDown
                                             , downCounter = 0 }
          | state == HalfDown =
              if | counter + 1 >= downThreshold -> ns { downState = NearlyDown
                                                      , downCounter = 0 }
                 | otherwise                    -> ns { downCounter = counter + 1}
          | state == NearlyDown =
              case downNodes of
                (_:_:_) -> ns { downCounter = 0 }
                [_]     -> maybeFailover (counter + 1) ns
                _       -> error "impossible"
          | state == Failover = ns
          | otherwise = error "impossible"
          where state   = downState ns
                counter = downCounter ns

        maybeFailover newCounter ns
          | newCounter >= downGracePeriod = ns { downState = Failover }
          | otherwise                     = ns { downCounter = newCounter }

getActions :: AF [Action]
getActions = sort <$> doGetActions

doGetActions :: AF [Action]
doGetActions = do
  downStates <- filter isDown <$> gets nodeStates

  case downStates of
    [down] | downState down == Failover -> handleFailover down
           | downState down == NearlyDown -> handleNearlyDown down
    _      -> handleOther downStates

  where isDown = not . (`elem` [Up, New]) . downState

        handleFailover down =
          do clusterSize  <- length <$> gets nodeStates
             mailedNodes  <- gets mailedTooSmallCluster
             currentNodes <- sort <$> getNodes

             if | clusterSize > minClusterSize -> return [(DoFailover, name down)]
                | mailedNodes == currentNodes -> return []
                | otherwise ->
                    do modify $ \state ->
                         state { mailedTooSmallCluster = currentNodes }
                       return [(DoMailTooSmall, name down)]

        handleNearlyDown _ = return []

        handleOther downStates = catMaybes <$> mapM mailWarning downStates
          where mailWarning down
                  | downState down == NearlyDown &&
                    not (mailedDownWarning down) =
                      do modifyNodeStates $ \states ->
                           (down { mailedDownWarning = True }) : delete down states
                         return $ Just (DoMailDownWarning, name down)
                  | otherwise = return Nothing

-- internal
getNodes :: AF [NodeId]
getNodes = map name <$> gets nodeStates

modifyNodeStatesM :: ([NodeState] -> AF [NodeState]) -> AF ()
modifyNodeStatesM f = do
  new <- gets nodeStates >>= f
  modify $ doModify new

  where doModify new state = state { nodeStates = new }

modifyNodeStates :: ([NodeState] -> [NodeState]) -> AF ()
modifyNodeStates f = modifyNodeStatesM (return . f)

-- tests
type Prob = Double

failProb :: Int -> Prob
failProb 0 = 0.05
failProb n = failProb (n - 1) / 2

recoverProb :: Prob
recoverProb = 0.1

addNodeProb :: Prob
addNodeProb = 0.01

removeNodeProb :: Prob
removeNodeProb = 0.05

data EventType = AddNode | RemoveNode | Fail | Recover
               deriving (Generic, Pretty)

instance Show EventType where
  show = prettyShow

type Time = Int

data Event = Event { ts   :: Time
                   , typ  :: EventType
                   , node :: NodeId
                   }
           deriving (Generic, Pretty)

instance Show Event where
  show = prettyShow

type History = (Time, [Event])

data GenState = GenState { tick        :: Time
                         , nextId      :: NodeId
                         , nodes       :: [NodeId]
                         , failedNodes :: [NodeId]
                         }
type GenMonad a = StateT GenState Gen a

genHistory :: Int -> Int -> Gen History
genHistory numNodes numTicks = evalStateT go (GenState 0 0 [] [])
  where go :: GenMonad History
        go =
          do initial <- replicateM numNodes addNode
             rest <- concat <$> replicateM numTicks step
             return (numTicks, initial ++ rest)

        step :: GenMonad [Event]
        step =
          do incTick
             concat <$> sequence [stepAdd, stepRemove, stepFail, stepRecover]

        stepAdd :: GenMonad [Event]
        stepAdd = withProb addNodeProb addNode

        stepRemove :: GenMonad [Event]
        stepRemove =
          do nodes <- gets nodes

             case nodes of
               [_] -> return []
               _   -> withProb removeNodeProb removeNode

        stepFail :: GenMonad [Event]
        stepFail =
          do live <- gets liveNodes
             numFailed <- length <$> gets failedNodes

             concat <$>
               forM live (withProb (failProb numFailed) . failNode)

        stepRecover :: GenMonad [Event]
        stepRecover =
          do nodes <- gets failedNodes
             concat <$>
               forM nodes (withProb recoverProb . recoverNode)

        withProb :: Prob -> GenMonad Event -> GenMonad [Event]
        withProb prob a =
          do die <- lift $ choose (0, 1)
             if | die < prob -> (:[]) <$> a
                | otherwise  -> return []

        liveNodes :: GenState -> [NodeId]
        liveNodes GenState{..} = nodes \\ failedNodes

        getNextId :: GenMonad Int
        getNextId =
          do nid <- gets nextId
             modify $ \s -> s { nextId = nid + 1 }
             return nid

        incTick :: GenMonad ()
        incTick = modify $ \s@(GenState {..}) -> s { tick = tick + 1}

        event :: EventType -> NodeId -> GenMonad Event
        event typ nid = gets tick >>= \t -> return $ Event t typ nid

        addNode :: GenMonad  Event
        addNode =
          do nid <- getNextId
             modify $ \s@(GenState{..}) -> s { nodes = nid : nodes }

             event AddNode nid

        removeNode :: GenMonad Event
        removeNode =
          do nid <- lift . elements =<< gets nodes
             modify $ \s@(GenState{..}) ->
               s { nodes = delete nid nodes
                 , failedNodes = delete nid failedNodes
                 }
             event RemoveNode nid

        failNode :: NodeId -> GenMonad Event
        failNode nid =
          do modify $ \s@(GenState{..}) ->
               s { failedNodes = nid : failedNodes }
             event Fail nid

        recoverNode :: NodeId -> GenMonad Event
        recoverNode nid =
          do modify $ \s@(GenState{..}) ->
               s { failedNodes = delete nid failedNodes }
             event Recover nid

instance Arbitrary History where
  arbitrary = sized $ \n ->
    do numNodes <- choose (1, n)
       let numTicks = numNodes * 10

       genHistory numNodes numTicks

  shrink (t, h) = [(t, h') | n <- nodes
                           , let h' = without n
                           , not $ null h']
    where nodes = nub $ sort $ map node h
          without n = filter (\e -> node e /= n) h

smallClusters :: Gen History
smallClusters = resize 5 arbitrary

type World = ([NodeId], [NodeId])

expandHistory :: History -> [(Time, World)]
expandHistory (numTicks, hist) = loop 0 [] [] hist
  where loop t nodes failed hist
          | t >= numTicks = []
          | otherwise = (t, w) : loop (t + 1) nodes' failed' rest
          where (es, rest) = span ((== t) . ts) hist
                w@(nodes', failed') = iter nodes failed es

        iter nodes failed [] = (nodes, failed)
        iter nodes failed (e:es) =
          case e of
              Event _ AddNode n -> iter (insSorted n nodes) failed es
              Event _ RemoveNode n -> iter (delete n nodes) (delete n failed) es
              Event _ Fail n -> iter nodes (insSorted n failed) es
              Event _ Recover n -> iter nodes (delete n failed) es

        insSorted x [] = [x]
        insSorted x (y : ys) | x > y = y : insSorted x ys
                             | otherwise = x : y : ys

run :: [(Time, World)] -> [(Time, [Action])]
run hist = evalAF (mapM process hist) defaultState
  where process (t, (nodes, failed)) = (t,) <$> processFrame nodes failed

runHistory :: History -> [(Time, [Action])]
runHistory = run . expandHistory

runModel :: History -> IO [(Time, [Action])]
runModel hist = do
  output <- readProcess "./autofailover.pl" [] input
  return $ zip ts $ map decodeLine (lines output)

  where (ts, worlds) = unzip $ expandHistory hist

        input = unlines $ map encodeFrame worlds
        encodeFrame (nodes, downNodes) =
          printf "frame(%s,%s)." (show nodes) (show downNodes)

        decodeLine line = sort $
          map (DoFailover,) failover ++
          map (DoMailTooSmall,) mailTooSmall ++
          map (DoMailDownWarning,) mailDown

          where (failover, mailTooSmall, mailDown) = read line

prop_checkByModel :: Property
prop_checkByModel = forAll smallClusters doCheck

  where doCheck hist = ioProperty $ do
          let result = runHistory hist
          modelResult <- runModel hist

          return $ result === modelResult
