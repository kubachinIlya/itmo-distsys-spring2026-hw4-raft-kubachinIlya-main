"""
Основной класс Raft сервера.
Реализует RPC обработчики и основную логику алгоритма.
"""
import random
import time
from typing import List, Optional, Dict, Callable, Any
from .types import (
    NodeState, LogEntry, RaftConfig,
    AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse
)
from .state import RaftState


class RaftNode:
    """Узел Raft алгоритма"""
    
    def __init__(self, config: RaftConfig, peer_ids: List[str]):
        self.config = config
        self.peer_ids = [p for p in peer_ids if p != config.node_id]
        self.state = RaftState(config)
        
        # Таймеры
        self._election_timeout_ms = self._random_election_timeout_ms()
        self._last_heartbeat_time = 0
        self._last_rpc_time_ms = 0  # симулированное время
        self._current_time_ms = 0   # текущее симулированное время
        
        # Состояние применения команд
        self.state_machine: Dict[str, Any] = {}  # простое хранилище key-value
        
        # Колбэки для системы
        self.on_state_change: Optional[Callable] = None
        self.on_commit: Optional[Callable] = None
    
    def _random_election_timeout_ms(self) -> float:
        """Сгенерировать случайный timeout выборов"""
        # TODO: сгенерировать случайный election_timeout_ms в диапазоне [min_ms, max_ms], используя self.config
        raise NotImplementedError()
    
    def tick(self, current_time_ms: float) -> None:
        """
        Выполнить one tick (шаг) сервера.
        Должна вызваться регулярно из main loop.
        """
        self._current_time_ms = current_time_ms  # запомнить текущее симулированное время
        time_since_last_rpc_ms = current_time_ms - self._last_rpc_time_ms
        
        if self.state.state == NodeState.LEADER:
            # Лидер должен отправлять heartbeats
            if time_since_last_rpc_ms >= self.config.heartbeat_interval_ms:
                # TODO: отправить heartbeats всем followers (использовать self._send_heartbeats для этого)
                # TODO: обновить self._last_rpc_time_ms после отправки heartbeats текущим симулированным временем
                pass
        
        elif self.state.state == NodeState.FOLLOWER:
            # Если timeout прошел, начать выборы
            if time_since_last_rpc_ms >= self._election_timeout_ms:
                # TODO: начать выборы (использовать self._start_election для этого)
                pass
        
        elif self.state.state == NodeState.CANDIDATE:
            # Candidate тоже может timeout и начать новые выборы
            if time_since_last_rpc_ms >= self._election_timeout_ms:
                # TODO: начать выборы (использовать self._start_election для этого)
                pass
    
    # ==================== RPC Handlers ====================
    
    def append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        """
        RPC: AppendEntries
        
        Обработка запроса на добавление логов от лидера.
        """
        # Правило 1: если term < currentTerm, вернуть false
        if req.term < self.state.current_term:
            # TODO: вернуть неуспешный AppendEntriesResponse с текущим term-ом
            pass
        
        # Если term больше - обновить и стать follower
        if req.term > self.state.current_term:
            # TODO: обновить self.state с помощью методов set_term и become_follower
            pass
        
        # Сбросить election timer
        self._last_rpc_time_ms = self._current_time_ms
        self._election_timeout_ms = self._random_election_timeout_ms()
        
        # Правило 2: проверить, есть ли entry с индексом prev_log_index и term prev_log_term
        if req.prev_log_index > 0:
            if req.prev_log_index > len(self.state.log) - 1:
                # Нет такого index в логе
                return AppendEntriesResponse(
                    term=self.state.current_term,
                    success=False,
                    last_log_index=len(self.state.log) - 1
                )
            
            prev_entry = self.state.log[req.prev_log_index]
            if prev_entry.term != req.prev_log_term:
                # Conflict: обрезать лог
                self.state.truncate_log(req.prev_log_index)
                return AppendEntriesResponse(
                    term=self.state.current_term,
                    success=False,
                    last_log_index=len(self.state.log) - 1
                )
        
        # Правило 3: добавить новые entries
        if req.entries:
            # Обрезать конфликтующие entries
            start_index = req.prev_log_index + 1
            for i, entry in enumerate(req.entries):
                log_index = start_index + i
                if log_index < len(self.state.log):
                    if self.state.log[log_index].term != entry.term:
                        # Conflict - обрезать
                        self.state.truncate_log(log_index)
                        break
            
            # Добавить недостающие entries
            for entry in req.entries[len(self.state.log) - start_index:]:
                self.state.log.append(entry)
        
        # Правило 4: обновить commit_index
        if req.leader_commit > self.state.commit_index:
            self.state.commit_index = min(req.leader_commit, len(self.state.log) - 1)
            self._apply_committed_entries()
        
        return AppendEntriesResponse(
            term=self.state.current_term,
            success=True,
            last_log_index=len(self.state.log) - 1
        )
    
    def request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        """
        RPC: RequestVote
        
        Обработка запроса голоса от candidate.
        """
        # Правило 1: если term < currentTerm, вернуть false
        if req.term < self.state.current_term:
            # TODO: вернуть неуспешный RequestVoteResponse с текущим term-ом
            pass
        
        # Если term больше - стать follower
        if req.term > self.state.current_term:
            # TODO: обновить self.state с помощью методов set_term и become_follower
            pass
        
        # Правило 2: проверить, может ли быть дан голос
        vote_granted = False
        if self.state.voted_for is None or self.state.voted_for == req.candidate_id:
            # Проверить логи (Raft safety)
            candidate_is_up_to_date = (
                req.last_log_term > self.state.get_last_log_term() or
                (req.last_log_term == self.state.get_last_log_term() and
                 req.last_log_index >= self.state.get_last_log_index())
            )
            
            if candidate_is_up_to_date:
                self.state.voted_for = req.candidate_id
                self._last_rpc_time_ms = self._current_time_ms
                self._election_timeout_ms = self._random_election_timeout_ms()
                vote_granted = True
        
        return RequestVoteResponse(
            term=self.state.current_term,
            vote_granted=vote_granted
        )
    
    # ==================== Internal Methods ====================
    
    def _start_election(self) -> None:
        """Начать выборы"""
        term = self.state.become_candidate()
        self._last_rpc_time_ms = self._current_time_ms
        self._election_timeout_ms = self._random_election_timeout_ms()
        self._notify_state_change()
        
        # В реальной системе здесь отправляются RequestVote RPC
        # Симуляция будет в cluster.py
    
    def _send_heartbeats(self) -> None:
        """Отправить heartbeats всем followers (логирование)"""
        # В реальной системе здесь отправляются AppendEntries RPC
        # Симуляция будет в cluster.py
        pass
    
    def _apply_committed_entries(self) -> None:
        """Применить все закомиченные но не примененные entries"""
        while self.state.last_applied < self.state.commit_index:
            self.state.last_applied += 1
            entry = self.state.log[self.state.last_applied]
            self._apply_command(entry.command)
            
            if self.on_commit:
                self.on_commit(entry.index, entry.command)
    
    def _apply_command(self, command: Any) -> Any:
        """Применить команду к state machine"""
        if isinstance(command, dict):
            if command.get("type") == "set":
                key, value = command.get("key"), command.get("value")
                self.state_machine[key] = value
                return value
            elif command.get("type") == "get":
                return self.state_machine.get(command.get("key"))
            elif command.get("type") == "delete":
                key = command.get("key")
                return self.state_machine.pop(key, None)
        return None
    
    def _notify_state_change(self) -> None:
        """Уведомить о смене состояния"""
        if self.on_state_change:
            self.on_state_change(self.state.state)
    
    # ==================== Public API ====================
    
    def propose(self, command: Any) -> bool:
        """
        Предложить команду на добавление в лог.
        Работает только если сервер - leader.
        """
        if self.state.state != NodeState.LEADER:
            return False
        
        self.state.append_entry(self.state.current_term, command)
        # В реальной системе теперь надо отправить это followers
        return True
    
    def get_state(self) -> Dict[str, Any]:
        """Получить информацию о состоянии сервера"""
        return {
            "node_id": self.config.node_id,
            "state": self.state.state.value,
            "term": self.state.current_term,
            "log_length": len(self.state.log),
            "commit_index": self.state.commit_index,
            "last_applied": self.state.last_applied,
            "voted_for": self.state.voted_for,
            "state_machine": self.state_machine.copy(),
        }
