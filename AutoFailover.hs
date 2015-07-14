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
import Data.Foldable (toList)
import Data.List ((\\), sort, delete, nub)
import Data.Maybe (catMaybes, fromJust)
import Data.Sequence (Seq, (<|))
import qualified Data.Sequence as Seq
import GHC.Generics (Generic)
import Text.PrettyPrint.Generic (Pretty, prettyShow)

import Test.QuickCheck (Arbitrary(arbitrary, shrink), Gen, Property,
                        forAll, resize, sized, choose, elements)

import Debug.Trace

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
                deriving (Eq, Generic, Pretty)

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
getActions = do
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

checkProperty :: Int
              -> ([Action] -> Maybe [World] -> Bool)
              -> History
              -> Bool
checkProperty lookBehind prop = checkPropertyAcc lookBehind prop' ()
  where prop' actions past _ = (prop actions past, ())

checkPropertyAcc :: Int
                 -> ([Action] -> Maybe [World] -> acc -> (Bool, acc))
                 -> acc
                 -> History
                 -> Bool
checkPropertyAcc lookBehind prop z history =
  loop (zip worlds' actions') Seq.empty z
  where worlds  = expandHistory history
        actions = run worlds

        worlds'  = map snd worlds
        actions' = map snd actions

        push world seq
          | Seq.length seq < lookBehind = world <| seq
          | otherwise                   = world <| Seq.take (lookBehind - 1) seq

        loop [] _ _ = True
        loop ((w, a) : rest) pastWorlds acc = holds && loop rest pastWorlds' acc'
          where pastWorlds'   = push w pastWorlds
                maybeWorlds
                  | Seq.length pastWorlds' == lookBehind =
                      Just (toList pastWorlds')
                  | otherwise =
                      Nothing
                (holds, acc') = prop a maybeWorlds acc

checkSimplePropertyAcc :: ([Action] -> World -> acc -> (Bool, acc))
                       -> acc
                       -> History
                       -> Bool
checkSimplePropertyAcc prop = checkPropertyAcc 1 prop'
  where prop' a ws = prop a (head $ fromJust ws)

checkSimpleProperty :: ([Action] -> World -> Bool) -> History -> Bool
checkSimpleProperty prop = checkSimplePropertyAcc prop' ()
  where prop' actions past _ = (prop actions past, ())

actionIs :: Action -> ActionType -> Bool
actionIs action atype = actionType action == atype

-- actual properties

-- check that we mail "too small" warning when the cluster size is less or
-- equal minClusterSize
prop_mailTooSmallClusterSize :: Property
prop_mailTooSmallClusterSize =
  forAll smallClusters (checkSimpleProperty prop)

  where prop actions (nodes, _)
          | hasTooSmall = length nodes <= minClusterSize
          | otherwise   = True
          where hasTooSmall = any (`actionIs` DoMailTooSmall) actions

-- check that we never send mail too small warning for more than one node
prop_mailTooSmallJustOne :: Property
prop_mailTooSmallJustOne =
  forAll smallClusters (checkSimpleProperty prop)

  where prop actions (nodes, _) = num == 0 || num == 1
          where warnings = filter (`actionIs` DoMailTooSmall) actions
                num      = length warnings

-- check that we mail the warning only if the node has been down for
-- failoverThreshold number of ticks
prop_mailTooSmallDown :: Property
prop_mailTooSmallDown =
  forAll smallClusters (checkProperty failoverThreshold prop)

  where prop actions maybeWorlds
          | Nothing <- maybeWorlds =
              null nodes
          | Just worlds <- maybeWorlds =
              and [isDown n w | n <- nodes, w <- worlds]
          where nodes = map actionNode $ filter (`actionIs` DoMailTooSmall) actions
                isDown n (_, failed) = n `elem` failed

-- check that we don't warn again unless the node configuration has changed
prop_mailTooSmallNoDups :: Property
prop_mailTooSmallNoDups =
  forAll smallClusters (checkSimplePropertyAcc prop [])

  where prop actions (nodes, _) seen
          | hasTooSmall = (nodes `notElem` seen, nodes : seen)
          | otherwise   = (True, seen)
          where hasTooSmall = any (`actionIs` DoMailTooSmall) actions

-- mail the warning if the node has been down for a failoverThreshold number
-- of frames and the node configuration haven't been seen before
prop_mailTooSmallWhenDown :: Property
prop_mailTooSmallWhenDown =
  forAll smallClusters (checkPropertyAcc failoverThreshold prop [])

  where prop actions maybePast seen
          | Nothing <- maybePast = (null mailNodes, seen)
          | Just past <- maybePast =
              let (nodes, _)   = head past
                  downNodes    = [n | n <- nodes, all (isDown n) past]
                  gracePeriod  = and [length f == 1 |
                                      (_, f) <- take downGracePeriod past]
                  nodesChanged = any ((/= nodes) . fst) past
                  seen'        = nodes : seen
              in if | not gracePeriod -> (null mailNodes, seen)
                    | nodes `elem` seen -> (null mailNodes, seen)
                    | length nodes > minClusterSize -> (null mailNodes, seen)
                    | nodesChanged -> (null mailNodes, seen)
                    | null mailNodes -> (null downNodes, seen)
                    | otherwise -> (sort downNodes == sort mailNodes, seen')
          where mailNodes =
                  map actionNode $ filter (`actionIs` DoMailTooSmall) actions

        isDown n (_, failed) = n `elem` failed
